use super::{
    Ack, Command, GenerationCommitted, Keys, Link, Outcome, Rejection, Result, Route,
    SyncRequested,
    auth::{Authentication, Envelope, Receiver, decimal, decode, encode, random},
    invalid, unix_seconds,
};
use hmac::Mac;
use serde::{Deserialize, Serialize};
use std::{future::Future, io, net::SocketAddr, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Semaphore,
    time::timeout,
};

const MAX_HEAD: usize = 8 * 1024;
const MAX_BODY: usize = 16 * 1024;
const HEADER_TIMEOUT: Duration = Duration::from_secs(3);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(8);
const CONNECTIONS: usize = 16;

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Identity {
    epoch: String,
}

pub fn address(value: &str) -> Result<SocketAddr> {
    let address: SocketAddr = value.parse()?;
    if address.ip().is_unspecified() || address.ip().is_multicast() || address.port() == 0 {
        return Err(invalid("control address requires a specific interface and port").into());
    }
    Ok(address)
}

pub async fn serve<H, F>(listener: TcpListener, link: Link, keys: Keys, handler: H) -> Result<()>
where
    H: Fn(Command) -> F + Clone + Send + Sync + 'static,
    F: Future<Output = std::result::Result<Outcome, Rejection>> + Send,
{
    let receiver = Arc::new(Receiver::new(link, keys)?);
    let connections = Arc::new(Semaphore::new(CONNECTIONS));
    loop {
        let (mut stream, _) = listener.accept().await?;
        let Ok(permit) = connections.clone().try_acquire_owned() else {
            continue;
        };
        let receiver = receiver.clone();
        let handler = handler.clone();
        tokio::spawn(async move {
            let _permit = permit;
            let result = timeout(REQUEST_TIMEOUT, handle(&mut stream, &receiver, handler)).await;
            if matches!(result, Ok(Err(_))) {
                let _ = timeout(Duration::from_secs(1), error_response(&mut stream, 400)).await;
            }
        });
    }
}

async fn handle<H, F>(stream: &mut TcpStream, receiver: &Receiver, handler: H) -> Result<()>
where
    H: FnOnce(Command) -> F,
    F: Future<Output = std::result::Result<Outcome, Rejection>>,
{
    let head = read_head(stream).await?;
    let mut fields = [httparse::EMPTY_HEADER; 32];
    let mut request = httparse::Request::new(&mut fields);
    let httparse::Status::Complete(offset) = request.parse(&head)? else {
        return Err(invalid("incomplete control request").into());
    };
    if request.version != Some(1) || request.method != Some("POST") {
        return Err(invalid("unsupported control method or HTTP version").into());
    }
    let route = request
        .path
        .and_then(|path| Route::for_link(path, receiver.link))
        .ok_or_else(|| invalid("unknown control path"))?;
    let fields = Headers(request.headers);
    fields.required("host")?;
    let length = fields.body_length()?;
    let (caller, audience) = receiver.link.roles();
    if fields.required("x-rfc-caller")? != caller || fields.required("x-rfc-audience")? != audience
    {
        return error_response(stream, 401).await;
    }
    let envelope = Envelope {
        route,
        epoch: fields.required("x-rfc-epoch")?,
        timestamp: fields.required("x-rfc-timestamp")?,
        nonce: fields.required("x-rfc-nonce")?,
    };
    let body = read_body(stream, &head[offset..], length).await?;
    let key = match receiver.authenticate(
        fields.required("x-rfc-key-id")?,
        &envelope,
        &body,
        fields.required("x-rfc-signature")?,
    ) {
        Ok(key) => key,
        Err(error) => {
            return error_response(
                stream,
                match error {
                    Authentication::StaleEpoch | Authentication::Reused => 409,
                    Authentication::Full => 503,
                    Authentication::Invalid => 401,
                },
            )
            .await;
        }
    };
    let response = if route == Route::Identify {
        if body != b"{}" {
            return Err(invalid("invalid identify body").into());
        }
        serde_json::to_vec(&Identity {
            epoch: receiver.epoch.clone(),
        })?
    } else {
        let command = Command::decode(route, &body)?;
        let event_id = command.event_id();
        let outcome = match handler(command).await {
            Ok(outcome) => outcome,
            Err(Rejection::Conflict) => return error_response(stream, 409).await,
            Err(Rejection::Unavailable) => return error_response(stream, 503).await,
        };
        serde_json::to_vec(&Ack { event_id, outcome })?
    };
    let nonce = envelope.nonce;
    let signature = encode(
        &key.response(receiver.link, nonce, 200, &response)
            .finalize()
            .into_bytes(),
    );
    let head = format!(
        "HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: {}\r\nX-RFC-Request-Nonce: {nonce}\r\nX-RFC-Receiver: {audience}\r\nX-RFC-Response-Signature: {signature}\r\n\r\n",
        response.len()
    );
    stream.write_all(head.as_bytes()).await?;
    stream.write_all(&response).await?;
    Ok(())
}

async fn error_response(stream: &mut TcpStream, status: u16) -> Result<()> {
    let message =
        format!("HTTP/1.1 {status} Rejected\r\nConnection: close\r\nContent-Length: 0\r\n\r\n");
    stream.write_all(message.as_bytes()).await?;
    Ok(())
}

struct Headers<'a>(&'a [httparse::Header<'a>]);

impl<'a> Headers<'a> {
    fn get(&self, name: &str) -> io::Result<Option<&'a str>> {
        let mut values = self
            .0
            .iter()
            .filter(|header| header.name.eq_ignore_ascii_case(name));
        let Some(header) = values.next() else {
            return Ok(None);
        };
        if values.next().is_some() || !header.value.iter().all(|byte| (b' '..=b'~').contains(byte))
        {
            return Err(invalid("duplicate or invalid control header"));
        }
        Ok(Some(
            std::str::from_utf8(header.value).map_err(io::Error::other)?,
        ))
    }

    fn required(&self, name: &str) -> io::Result<&'a str> {
        self.get(name)?
            .filter(|value| !value.is_empty())
            .ok_or_else(|| invalid("missing control header"))
    }

    fn body_length(&self) -> io::Result<usize> {
        if self.required("content-type")? != "application/json"
            || ["transfer-encoding", "content-encoding", "upgrade", "expect"]
                .iter()
                .any(|name| {
                    self.0
                        .iter()
                        .any(|header| header.name.eq_ignore_ascii_case(name))
                })
        {
            return Err(invalid("unsupported control framing"));
        }
        let length = usize::try_from(decimal(self.required("content-length")?)?)
            .map_err(io::Error::other)?;
        if length > MAX_BODY {
            return Err(invalid("control body exceeds limit"));
        }
        Ok(length)
    }
}

async fn read_head(stream: &mut TcpStream) -> Result<Vec<u8>> {
    timeout(HEADER_TIMEOUT, async {
        let mut head = Vec::with_capacity(1024);
        let mut buffer = [0; 1024];
        while head.len() < MAX_HEAD {
            let size = stream.read(&mut buffer).await?;
            if size == 0 {
                break;
            }
            let start = head.len().saturating_sub(3);
            head.extend_from_slice(&buffer[..size]);
            if let Some(end) = head[start..]
                .windows(4)
                .position(|bytes| bytes == b"\r\n\r\n")
            {
                let end = start + end + 4;
                let strict = head[..end]
                    .iter()
                    .enumerate()
                    .all(|(index, byte)| match byte {
                        b'\n' => index > 0 && head[index - 1] == b'\r',
                        b'\r' => head.get(index + 1) == Some(&b'\n'),
                        _ => true,
                    });
                if end <= MAX_HEAD && strict {
                    return Ok(head);
                }
                break;
            }
        }
        Err(invalid("invalid or oversized control head").into())
    })
    .await?
}

async fn read_body(stream: &mut TcpStream, buffered: &[u8], length: usize) -> Result<Vec<u8>> {
    if buffered.len() > length {
        return Err(invalid("control body exceeds the declared length").into());
    }
    let mut body = vec![0; length];
    body[..buffered.len()].copy_from_slice(buffered);
    stream.read_exact(&mut body[buffered.len()..]).await?;
    Ok(body)
}

pub struct Client {
    address: SocketAddr,
    link: Link,
    keys: Keys,
    epoch: Option<String>,
}

impl Client {
    pub fn new(address: SocketAddr, link: Link, keys: Keys) -> Self {
        Self {
            address,
            link,
            keys,
            epoch: None,
        }
    }

    pub async fn request_sync(&mut self, event: &SyncRequested) -> Result<()> {
        if self.link != Link::FeedSync {
            return Err(invalid("wrong control link").into());
        }
        self.deliver(Route::Sync, event.event_id, serde_json::to_vec(event)?)
            .await
    }

    pub async fn notify_generation(&mut self, event: &GenerationCommitted) -> Result<()> {
        if self.link != Link::SyncServe {
            return Err(invalid("wrong control link").into());
        }
        self.deliver(
            Route::Generation,
            event.event_id,
            serde_json::to_vec(event)?,
        )
        .await
    }

    async fn deliver(&mut self, route: Route, event_id: uuid::Uuid, body: Vec<u8>) -> Result<()> {
        if self.epoch.is_none() {
            let response = self.exchange(Route::Identify, "bootstrap", b"{}").await?;
            let identity: Identity = serde_json::from_slice(&response)?;
            let _: [u8; 32] = decode(&identity.epoch)?;
            self.epoch = Some(identity.epoch);
        }
        let epoch = self.epoch.take().expect("identified receiver");
        let response = self.exchange(route, &epoch, &body).await?;
        let ack: Ack = serde_json::from_slice(&response)?;
        if ack.event_id != event_id
            || !matches!(
                (route, ack.outcome),
                (Route::Sync, Outcome::Accepted)
                    | (Route::Generation, Outcome::Observed | Outcome::Superseded)
            )
        {
            return Err(invalid("invalid control acknowledgment").into());
        }
        self.epoch = Some(epoch);
        Ok(())
    }

    async fn exchange(&self, route: Route, epoch: &str, body: &[u8]) -> Result<Vec<u8>> {
        if body.len() > MAX_BODY {
            return Err(invalid("control body exceeds limit").into());
        }
        timeout(REQUEST_TIMEOUT, async {
            let (caller, audience) = self.link.roles();
            let key = &self.keys.active;
            let timestamp = unix_seconds()?.to_string();
            let nonce = encode(&random()?);
            let envelope = Envelope { epoch, timestamp: &timestamp, nonce: &nonce, route };
            let tag = encode(&key.request(self.link, &envelope, body).finalize().into_bytes());
            let head = format!(
                "POST {} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: {}\r\nX-RFC-Key-Id: {}\r\nX-RFC-Caller: {caller}\r\nX-RFC-Audience: {audience}\r\nX-RFC-Epoch: {epoch}\r\nX-RFC-Timestamp: {timestamp}\r\nX-RFC-Nonce: {nonce}\r\nX-RFC-Signature: {tag}\r\n\r\n",
                route.path(), self.address, body.len(), key.id
            );
            let mut stream = TcpStream::connect(self.address).await?;
            stream.write_all(head.as_bytes()).await?;
            stream.write_all(body).await?;
            let head = read_head(&mut stream).await?;
            let mut headers = [httparse::EMPTY_HEADER; 32];
            let mut response = httparse::Response::new(&mut headers);
            let httparse::Status::Complete(offset) = response.parse(&head)? else {
                return Err(invalid("incomplete control response").into());
            };
            if response.version != Some(1) || response.code != Some(200) {
                return Err(invalid("receiver rejected control message").into());
            }
            let fields = Headers(response.headers);
            if fields.required("x-rfc-request-nonce")? != nonce ||
                fields.required("x-rfc-receiver")? != audience {
                return Err(invalid("unexpected acknowledgment nonce or receiver").into());
            }
            let tag: [u8; 48] = decode(fields.required("x-rfc-response-signature")?)?;
            let body = read_body(&mut stream, &head[offset..], fields.body_length()?).await?;
            if stream.read(&mut [0; 1]).await? != 0 {
                return Err(invalid("control response exceeds the declared length").into());
            }
            key.response(self.link, &nonce, 200, &body)
                .verify_slice(&tag)
                .map_err(|_| invalid("invalid control response MAC"))?;
            Ok(body)
        }).await?
    }
}
