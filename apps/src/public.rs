use std::{
    collections::HashMap,
    env, fs, io,
    net::{IpAddr, SocketAddr},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use axum::{
    Extension, Router,
    body::{Body, to_bytes},
    extract::{Request, State},
    http::{HeaderMap, StatusCode, header, uri::Authority},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::get,
};
use hyper::server::conn::http1;
use hyper_util::{
    rt::{TokioIo, TokioTimer},
    service::TowerToHyperService,
};
use rfc_mcp_docker::{
    ProbeResult,
    control::{self, Command, Keys, Link, Outcome, Rejection},
};
use rfc_search::{Rg, Search, SearchError};
use rfc_storage::postgres::{GenerationSelection, Reader};
use rmcp::{
    ErrorData, RoleServer, ServerHandler,
    model::{
        CallToolRequestParams, CallToolResponse, CallToolResult, CustomRequest, CustomResult,
        ListToolsResult, PaginatedRequestParams, ProtocolVersion, RequestId, ServerCapabilities,
        ServerConfig, Tool, ToolAnnotations,
    },
    service::RequestContext,
};
use serde::{Deserialize, de::DeserializeOwned};
use serde_json::{Map, Value, json};
use tokio::{
    net::TcpListener,
    sync::Semaphore,
    time::{Instant, timeout, timeout_at},
};

const RESPONSE_LIMIT: usize = 256 * 1024;
const SEARCH_TAIL_RESERVE: usize = 4096;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(15);
const HEADER_TIMEOUT: Duration = Duration::from_secs(5);
const RATE: f64 = 5.0;
const BURST: f64 = 10.0;
const BUCKETS: usize = 4096;
const BUCKET_TTL: Duration = Duration::from_secs(60);
const REQUESTS: usize = 16;
const CONNECTIONS: usize = 128;
static VERSIONS: [ProtocolVersion; 1] = [ProtocolVersion::V_2026_07_28];

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ToolSchema {
    name: String,
    description: String,
    input_schema: Map<String, Value>,
    output_schema: Map<String, Value>,
}

#[derive(Clone)]
struct Mcp {
    search: Arc<Search>,
    rg: Arc<Rg>,
    tools: Arc<Vec<Tool>>,
}

impl Mcp {
    fn new(key: &[u8; 48], rg: Arc<Rg>) -> Result<Self, serde_json::Error> {
        let definitions: Vec<ToolSchema> =
            serde_json::from_str(include_str!("../schemas/tools.json"))?;
        let tools = definitions
            .into_iter()
            .map(|definition| {
                Tool::new(
                    definition.name,
                    definition.description,
                    definition.input_schema,
                )
                .with_raw_output_schema(Arc::new(definition.output_schema))
                .with_annotations(
                    ToolAnnotations::new()
                        .read_only(true)
                        .destructive(false)
                        .idempotent(true)
                        .open_world(false),
                )
            })
            .collect();
        Ok(Self {
            search: Arc::new(Search::new(key)),
            rg,
            tools: Arc::new(tools),
        })
    }

    async fn call(&self, name: &str, args: Value, id: &RequestId) -> Result<Value, SearchError> {
        let fits = |value: &Value| wire_size(id, value) <= RESPONSE_LIMIT;
        match name {
            "rg" => {
                let id = id.clone();
                self.rg
                    .clone()
                    .run(
                        self.search.clone(),
                        arguments(args)?,
                        move |value, reserve| wire_size(&id, value) <= RESPONSE_LIMIT - reserve,
                    )
                    .await
            }
            "list" => self.search.list(arguments(args)?, fits).await,
            "get" => self.search.get(arguments(args)?, fits).await,
            "table" => self.search.table(arguments(args)?, fits).await,
            "search" => {
                let page = self
                    .search
                    .run_with_budget(arguments(args)?, |page| {
                        wire_size(id, &json!(page)) <= RESPONSE_LIMIT - SEARCH_TAIL_RESERVE
                    })
                    .await?;
                Ok(json!(page))
            }
            _ => unreachable!("only implemented tools reach corpus dispatch"),
        }
    }
}

impl ServerHandler for Mcp {
    fn get_info(&self) -> ServerConfig {
        ServerConfig::new(ServerCapabilities::builder().enable_tools().build())
            .with_protocol_version(ProtocolVersion::V_2026_07_28)
    }

    fn supported_protocol_versions(&self) -> std::borrow::Cow<'static, [ProtocolVersion]> {
        std::borrow::Cow::Borrowed(&VERSIONS)
    }

    fn get_tool(&self, name: &str) -> Option<Tool> {
        self.tools.iter().find(|tool| tool.name == name).cloned()
    }

    async fn on_custom_request(
        &self,
        request: CustomRequest,
        _: RequestContext<RoleServer>,
    ) -> Result<CustomResult, ErrorData> {
        if matches!(
            request.method.as_ref(),
            "server/discover" | "tools/list" | "tools/call"
        ) {
            return Err(ErrorData::invalid_params(
                "Invalid MCP method parameters",
                None,
            ));
        }
        Err(ErrorData::new(
            rmcp::model::ErrorCode::METHOD_NOT_FOUND,
            "Unknown MCP method",
            None,
        ))
    }

    async fn list_tools(
        &self,
        request: Option<PaginatedRequestParams>,
        _: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, ErrorData> {
        if request.and_then(|request| request.cursor).is_some() {
            return Err(ErrorData::invalid_params(
                "The tool catalog has no continuation",
                None,
            ));
        }
        Ok(ListToolsResult::with_all_items(self.tools.as_ref().clone()))
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, ErrorData> {
        if self.get_tool(&request.name).is_none() {
            return Err(ErrorData::invalid_params("Unknown tool", None));
        }
        let args = Value::Object(request.arguments.unwrap_or_default());
        let started = Instant::now();
        let budget = context
            .extensions
            .get::<axum::http::request::Parts>()
            .and_then(|parts| parts.extensions.get::<RequestBudget>())
            .copied();
        let deadline = budget.map_or_else(
            || started + REQUEST_TIMEOUT,
            |budget| budget.deadline - Duration::from_millis(100),
        );
        let source_url = if matches!(request.name.as_ref(), "get" | "table") {
            args.get("id")
                .and_then(Value::as_i64)
                .filter(|id| (1..=i64::from(i32::MAX)).contains(id))
                .map(|id| format!("https://www.rfc-editor.org/rfc/rfc{id}.txt"))
        } else {
            None
        };
        let output = tokio::select! {
            () = context.ct.cancelled() => return Err(ErrorData::internal_error("Request cancelled", None)),
            result = timeout_at(deadline, self.call(&request.name, args, &context.id)) => {
                result.unwrap_or(Err(SearchError::Deadline))
            }
        };
        let result = match output {
            Ok(value) if wire_size(&context.id, &value) <= RESPONSE_LIMIT => {
                CallToolResult::structured(value)
            }
            Ok(_) => tool_error(SearchError::ResourceLimit, source_url),
            Err(error) => tool_error(error, source_url),
        };
        if let Some(budget) = budget {
            let data = result.structured_content.as_ref();
            let code = data
                .and_then(|value| value["code"].as_str())
                .unwrap_or("ok");
            let generation = data
                .and_then(|value| value["generation_id"].as_str())
                .unwrap_or("-");
            eprintln!(
                "mcp request={} tool={} outcome={} generation={} elapsed_ms={}",
                budget.serial,
                request.name,
                code,
                generation,
                started.elapsed().as_millis()
            );
        }
        Ok(result.into())
    }
}

fn arguments<T: DeserializeOwned>(value: Value) -> Result<T, SearchError> {
    if value
        .as_object()
        .is_some_and(|args| args.values().any(Value::is_null))
    {
        return Err(SearchError::InvalidArguments);
    }
    serde_json::from_value(value).map_err(|_| SearchError::InvalidArguments)
}

fn wire_size(id: &RequestId, value: &Value) -> usize {
    let result = CallToolResult::structured(value.clone());
    serde_json::to_vec(&json!({"jsonrpc":"2.0","id":id,"result":result}))
        .map_or(usize::MAX, |body| body.len())
}

fn tool_error(error: SearchError, source_url: Option<String>) -> CallToolResult {
    let mut value = json!({"code":error.code(),"message":error.to_string()});
    match error {
        SearchError::Query(error) => value["span"] = json!([error.span.start, error.span.end]),
        SearchError::AmbiguousSection(ids) => value["candidate_section_ids"] = json!(ids),
        SearchError::SourcePolicyRestricted
        | SearchError::SourceUnavailable
        | SearchError::ResourceLimit
            if source_url.is_some() =>
        {
            value["source_url"] = json!(source_url)
        }
        _ => {}
    }
    CallToolResult::structured_error(value)
}

#[derive(Clone, Copy)]
struct Peer(IpAddr);

#[derive(Clone, Copy)]
struct RequestBudget {
    serial: u64,
    deadline: Instant,
}

struct Bucket {
    tokens: f64,
    seen: Instant,
}

struct Admission {
    rg: Arc<Rg>,
    in_flight: Semaphore,
    sources: Mutex<HashMap<IpAddr, Bucket>>,
    proxies: Vec<IpAddr>,
    hosts: Vec<Authority>,
    origins: Vec<String>,
    requests: AtomicU64,
    rejected: AtomicU64,
    responses: AtomicU64,
}

impl Admission {
    fn new(rg: Arc<Rg>, proxies: Vec<IpAddr>, hosts: Vec<Authority>, origins: Vec<String>) -> Self {
        Self {
            rg,
            in_flight: Semaphore::new(REQUESTS),
            sources: Mutex::default(),
            proxies,
            hosts,
            origins,
            requests: AtomicU64::new(0),
            rejected: AtomicU64::new(0),
            responses: AtomicU64::new(0),
        }
    }

    fn endpoint(&self, request: &Request) -> Result<(), StatusCode> {
        let mut origins = request.headers().get_all(header::ORIGIN).iter();
        if let Some(origin) = origins.next()
            && (origins.next().is_some()
                || !self
                    .origins
                    .iter()
                    .any(|allowed| origin.as_bytes() == allowed.as_bytes()))
        {
            return Err(StatusCode::FORBIDDEN);
        }
        let mut hosts = request.headers().get_all(header::HOST).iter();
        let host = hosts
            .next()
            .and_then(|value| value.to_str().ok())
            .and_then(authority)
            .ok_or(StatusCode::BAD_REQUEST)?;
        if hosts.next().is_some()
            || request
                .uri()
                .authority()
                .is_some_and(|authority| *authority != host)
        {
            return Err(StatusCode::BAD_REQUEST);
        }
        if self.hosts.iter().any(|allowed| {
            host.host().eq_ignore_ascii_case(allowed.host())
                && (allowed.port().is_none() || allowed.port_u16() == host.port_u16())
        }) {
            return Ok(());
        }
        Err(StatusCode::FORBIDDEN)
    }

    fn source(&self, peer: IpAddr, headers: &HeaderMap) -> Result<IpAddr, StatusCode> {
        if !self.proxies.contains(&peer) {
            return Ok(peer);
        }
        let mut values = headers.get_all("x-real-ip").iter();
        let source = values
            .next()
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<IpAddr>().ok())
            .map(|ip| ip.to_canonical());
        source
            .filter(|_| values.next().is_none())
            .ok_or(StatusCode::BAD_REQUEST)
    }

    fn take(&self, source: IpAddr, now: Instant) -> bool {
        let mut sources = self.sources.lock().expect("admission table lock");
        if !sources.contains_key(&source) && sources.len() >= BUCKETS {
            sources.retain(|_, bucket| now.duration_since(bucket.seen) < BUCKET_TTL);
            if sources.len() >= BUCKETS {
                return false;
            }
        }
        let bucket = sources.entry(source).or_insert(Bucket {
            tokens: BURST,
            seen: now,
        });
        let elapsed = now.saturating_duration_since(bucket.seen).as_secs_f64();
        bucket.tokens = (bucket.tokens + elapsed * RATE).min(BURST);
        bucket.seen = now;
        if bucket.tokens < 1.0 {
            return false;
        }
        bucket.tokens -= 1.0;
        true
    }

    fn overloaded(&self) -> Response {
        self.rejected.fetch_add(1, Ordering::Relaxed);
        (StatusCode::TOO_MANY_REQUESTS, [(header::RETRY_AFTER, "1")]).into_response()
    }
}

async fn admit(
    State(admission): State<Arc<Admission>>,
    mut request: Request,
    next: Next,
) -> Response {
    let serial = admission.requests.fetch_add(1, Ordering::Relaxed) + 1;
    let now = Instant::now();
    if let Err(status) = admission.endpoint(&request) {
        return status.into_response();
    }
    let peer = request
        .extensions()
        .get::<Peer>()
        .expect("listener supplies peer")
        .0;
    let source = match admission.source(peer, request.headers()) {
        Ok(source) => source,
        Err(status) => return status.into_response(),
    };
    let Ok(_permit) = admission.in_flight.try_acquire() else {
        return admission.overloaded();
    };
    if !admission.take(source, now) {
        return admission.overloaded();
    }
    let deadline = now + REQUEST_TIMEOUT;
    request
        .extensions_mut()
        .insert(RequestBudget { serial, deadline });
    let response = timeout_at(deadline, async {
        let (mut parts, body) = next.run(request).await.into_parts();
        let Ok(body) = to_bytes(body, RESPONSE_LIMIT).await else {
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        };
        parts.headers.insert(
            header::CACHE_CONTROL,
            header::HeaderValue::from_static("no-store"),
        );
        parts.headers.insert(
            header::X_CONTENT_TYPE_OPTIONS,
            header::HeaderValue::from_static("nosniff"),
        );
        eprintln!(
            "mcp request={serial} status={} bytes={} elapsed_ms={}",
            parts.status.as_u16(),
            body.len(),
            now.elapsed().as_millis()
        );
        Response::from_parts(parts, Body::from(body))
    })
    .await
    .unwrap_or_else(|_| StatusCode::GATEWAY_TIMEOUT.into_response());
    admission.responses.fetch_add(1, Ordering::Relaxed);
    response
}

async fn ready(State(state): State<Arc<Admission>>) -> StatusCode {
    if !state.rg.available() {
        return StatusCode::SERVICE_UNAVAILABLE;
    }
    let ready = timeout(Duration::from_secs(2), async {
        Reader::connect()
            .await?
            .with_generation(GenerationSelection::Active, async |_| Ok(()))
            .await
    })
    .await;
    if matches!(ready, Ok(Ok(()))) {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

async fn metrics(State(state): State<Arc<Admission>>) -> String {
    format!(
        "rfc_mcp_requests_total {}\nrfc_mcp_admission_rejected_total {}\nrfc_mcp_responses_total {}\nrfc_mcp_in_flight {}\nrfc_mcp_rg_available {}\n",
        state.requests.load(Ordering::Relaxed),
        state.rejected.load(Ordering::Relaxed),
        state.responses.load(Ordering::Relaxed),
        REQUESTS - state.in_flight.available_permits(),
        u8::from(state.rg.available()),
    )
}

fn setting(name: &str, default: &str) -> io::Result<String> {
    match env::var(name) {
        Ok(value) => Ok(value),
        Err(env::VarError::NotPresent) => Ok(default.into()),
        Err(_) => Err(invalid(name)),
    }
}

fn invalid(name: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, format!("invalid {name}"))
}

fn authority(value: &str) -> Option<Authority> {
    if value.contains(['@', '*']) {
        return None;
    }
    let authority = value.parse::<Authority>().ok()?;
    let host = authority.host();
    if host.is_empty() || (value != host && authority.port_u16().is_none()) {
        return None;
    }
    Some(authority)
}

fn settings(name: &str) -> io::Result<Vec<String>> {
    let value = setting(name, "")?;
    if value.is_empty() {
        return Ok(Vec::new());
    }
    value
        .split(',')
        .map(|item| {
            let item = item.trim();
            if item.is_empty() {
                Err(invalid(name))
            } else {
                Ok(item.into())
            }
        })
        .collect()
}

pub async fn run() -> ProbeResult {
    let public_addr: SocketAddr = setting("RFC_MCP_BIND", "127.0.0.1:8780")?
        .parse()
        .map_err(|_| invalid("RFC_MCP_BIND"))?;
    let admin_addr: SocketAddr = setting("RFC_MCP_ADMIN_BIND", "127.0.0.1:8781")?
        .parse()
        .map_err(|_| invalid("RFC_MCP_ADMIN_BIND"))?;
    if !admin_addr.ip().is_loopback() {
        return Err(invalid("RFC_MCP_ADMIN_BIND (loopback required)").into());
    }
    let hosts = settings("RFC_MCP_HOSTS")?;
    let allowed = hosts
        .iter()
        .map(|host| authority(host))
        .collect::<Option<Vec<_>>>()
        .filter(|hosts| !hosts.is_empty())
        .ok_or_else(|| invalid("RFC_MCP_HOSTS"))?;
    let origins = settings("RFC_MCP_ORIGINS")?;
    for origin in &origins {
        let url = reqwest::Url::parse(origin).map_err(|_| invalid("RFC_MCP_ORIGINS"))?;
        if !matches!(url.scheme(), "http" | "https")
            || !url.has_host()
            || url.origin().ascii_serialization() != *origin
        {
            return Err(invalid("RFC_MCP_ORIGINS").into());
        }
    }
    let proxies = settings("RFC_MCP_TRUSTED_PROXIES")?
        .iter()
        .map(|proxy| {
            proxy
                .parse::<IpAddr>()
                .map(|ip| ip.to_canonical())
                .map_err(|_| invalid("RFC_MCP_TRUSTED_PROXIES"))
        })
        .collect::<io::Result<Vec<_>>>()?;
    let key_path = setting("RFC_MCP_CURSOR_KEY_FILE", "/run/secrets/rfc_cursor_key")?;
    let key: [u8; 48] = fs::read(key_path)
        .map_err(|_| invalid("RFC_MCP_CURSOR_KEY_FILE"))?
        .try_into()
        .map_err(|_| invalid("RFC_MCP_CURSOR_KEY_FILE (48 raw bytes required)"))?;
    let rg = Arc::new(Rg::initialize().await);
    let admission = Arc::new(Admission::new(
        rg.clone(),
        proxies,
        allowed,
        origins.clone(),
    ));
    let public = super::mcp::transport(Mcp::new(&key, rg)?, Some(hosts), origins)
        .route_layer(middleware::from_fn_with_state(admission.clone(), admit));
    let admin = Router::new()
        .route("/live", get(|| async { StatusCode::OK }))
        .route("/ready", get(ready))
        .route("/metrics", get(metrics))
        .with_state(admission);
    let notification = match std::env::var("RFC_SERVE_NOTIFY_BIND") {
        Ok(bind) => {
            let keys = Keys::load(Link::SyncServe)?;
            if keys.contains(&key) {
                return Err(invalid("notification key overlaps the cursor key").into());
            }
            Some((TcpListener::bind(control::address(&bind)?).await?, keys))
        }
        Err(std::env::VarError::NotPresent) => None,
        Err(error) => return Err(error.into()),
    };
    let pub_listener = TcpListener::bind(public_addr).await?;
    let admin_listener = TcpListener::bind(admin_addr).await?;
    eprintln!("RFC MCP listening on {}", pub_listener.local_addr()?);
    tokio::try_join!(
        async {
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(
                serve_public(pub_listener, public).await?,
            )
        },
        async { Ok(axum::serve(admin_listener, admin).await?) },
        async {
            match notification {
                Some((listener, keys)) => {
                    control::serve(listener, Link::SyncServe, keys, notify).await
                }
                None => std::future::pending().await,
            }
        },
    )?;
    Ok(())
}

async fn notify(command: Command) -> Result<Outcome, Rejection> {
    let Command::Generation(event) = command else {
        return Err(Rejection::Conflict);
    };
    let reader = Reader::connect()
        .await
        .map_err(|_| Rejection::Unavailable)?;
    let (active, sequence) = reader.active().await.map_err(|_| Rejection::Unavailable)?;
    if sequence > event.publication_sequence {
        return Ok(Outcome::Superseded);
    }
    if sequence != event.publication_sequence
        || active.map(|id| id.get()) != u64::try_from(event.generation_id).ok()
    {
        return Err(Rejection::Conflict);
    }
    Ok(Outcome::Observed)
}

async fn serve_public(listener: TcpListener, router: Router) -> io::Result<()> {
    let connections = Arc::new(Semaphore::new(CONNECTIONS));
    loop {
        let (stream, peer) = listener.accept().await?;
        let Ok(permit) = connections.clone().try_acquire_owned() else {
            continue;
        };
        let router = router
            .clone()
            .layer(Extension(Peer(peer.ip().to_canonical())));
        tokio::spawn(async move {
            let _permit = permit;
            let service = TowerToHyperService::new(router);
            let _ = http1::Builder::new()
                .timer(TokioTimer::new())
                .header_read_timeout(HEADER_TIMEOUT)
                .max_buf_size(32 * 1024)
                .serve_connection(TokioIo::new(stream), service)
                .await;
        });
    }
}
