use super::{Link, Result, Route, invalid, unix_seconds};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use hmac::{Hmac, KeyInit, Mac};
use sha2::Sha384;
use std::{collections::HashMap, env, fs::File, io::Read, path::Path, sync::Mutex, time::Instant};

const SKEW: u64 = 60;
const NONCE_SECONDS: u64 = 150;
const NONCES: usize = 8_192;
const MAX_ROTATION_OVERLAP: u64 = 86_400;

pub struct Keys {
    pub(super) active: Key,
    previous: Option<(Key, u64)>,
}

pub(super) struct Key {
    pub(super) id: String,
    secret: [u8; 48],
}

fn load_key(id: String, path: &Path) -> Result<Key> {
    if id.is_empty()
        || id.len() > 32
        || !id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"_-".contains(&byte))
        || !path.is_absolute()
    {
        return Err(invalid("invalid control key ID or file path").into());
    }
    let file = File::open(path)?;
    if !file.metadata()?.is_file() {
        return Err(invalid("control key must be a regular file").into());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if file.metadata()?.mode() & 0o007 != 0 || file.metadata()?.mode() & 0o022 != 0 {
            return Err(invalid("control key file permissions are too broad").into());
        }
    }
    let mut bytes = Vec::with_capacity(49);
    file.take(49).read_to_end(&mut bytes)?;
    let secret = bytes
        .try_into()
        .map_err(|_| invalid("control key requires 48 raw bytes"))?;
    Ok(Key { id, secret })
}

impl Keys {
    pub fn load(link: Link) -> Result<Self> {
        let prefix = link.key_prefix();
        let setting = |suffix| env::var(format!("{prefix}_{suffix}"));
        let active = load_key(setting("KEY_ID")?, Path::new(&setting("KEY_FILE")?))?;
        let previous = match (
            setting("PREVIOUS_KEY_ID"),
            setting("PREVIOUS_KEY_FILE"),
            setting("PREVIOUS_KEY_EXPIRES"),
        ) {
            (
                Err(env::VarError::NotPresent),
                Err(env::VarError::NotPresent),
                Err(env::VarError::NotPresent),
            ) => None,
            (Ok(id), Ok(file), Ok(expires)) => {
                let expires = decimal(&expires)?;
                let previous = load_key(id, Path::new(&file))?;
                if previous.id == active.id
                    || previous.secret == active.secret
                    || expires > unix_seconds()?.saturating_add(MAX_ROTATION_OVERLAP)
                {
                    return Err(invalid("invalid control key rotation overlap").into());
                }
                Some((previous, expires))
            }
            _ => return Err(invalid("incomplete previous control key configuration").into()),
        };
        Ok(Self { active, previous })
    }

    pub fn contains(&self, secret: &[u8; 48]) -> bool {
        &self.active.secret == secret
            || self
                .previous
                .as_ref()
                .is_some_and(|(key, _)| &key.secret == secret)
    }

    pub fn overlaps(&self, other: &Self) -> bool {
        self.contains(&other.active.secret)
            || other
                .previous
                .as_ref()
                .is_some_and(|(key, _)| self.contains(&key.secret))
    }

    fn select(&self, id: &str, now: u64) -> Option<&Key> {
        if id == self.active.id {
            return Some(&self.active);
        }
        self.previous
            .as_ref()
            .filter(|(key, expires)| id == key.id && now < *expires)
            .map(|(key, _)| key)
    }
}

pub(super) fn decimal(value: &str) -> std::io::Result<u64> {
    if value.is_empty()
        || (value.starts_with('0') && value.len() > 1)
        || !value.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(invalid("noncanonical control number"));
    }
    value
        .parse()
        .map_err(|_| invalid("control number exceeds range"))
}

pub(super) fn random() -> Result<[u8; 32]> {
    let mut bytes = [0; 32];
    getrandom::fill(&mut bytes).map_err(std::io::Error::other)?;
    Ok(bytes)
}

pub(super) fn encode(bytes: &[u8]) -> String {
    URL_SAFE_NO_PAD.encode(bytes)
}

pub(super) fn decode<const N: usize>(value: &str) -> std::io::Result<[u8; N]> {
    if value.len() != (N * 8).div_ceil(6) {
        return Err(invalid("invalid control token length"));
    }
    let mut bytes = [0; N];
    let size = URL_SAFE_NO_PAD
        .decode_slice(value, &mut bytes)
        .map_err(|_| invalid("invalid control token encoding"))?;
    if size != N {
        return Err(invalid("invalid control token length"));
    }
    Ok(bytes)
}

pub(super) struct Envelope<'a> {
    pub(super) epoch: &'a str,
    pub(super) timestamp: &'a str,
    pub(super) nonce: &'a str,
    pub(super) route: Route,
}

impl Key {
    pub(super) fn request(&self, link: Link, envelope: &Envelope<'_>, body: &[u8]) -> Hmac<Sha384> {
        let (caller, audience) = link.roles();
        let Envelope {
            epoch,
            timestamp,
            nonce,
            route,
        } = envelope;
        let prefix = format!(
            "RFMCP-HMAC-SHA384-V1\n{}\n{caller}\n{audience}\n{epoch}\n{timestamp}\n{nonce}\nPOST\n{}\napplication/json\n{}\n\n",
            self.id,
            route.path(),
            body.len()
        );
        self.mac(prefix, body)
    }

    pub(super) fn response(
        &self,
        link: Link,
        nonce: &str,
        status: u16,
        body: &[u8],
    ) -> Hmac<Sha384> {
        let (caller, receiver) = link.roles();
        let prefix = format!(
            "RFMCP-HMAC-SHA384-RESPONSE-V1\n{}\n{receiver}\n{caller}\n{nonce}\n{status}\napplication/json\n{}\n\n",
            self.id,
            body.len()
        );
        self.mac(prefix, body)
    }

    fn mac(&self, prefix: String, body: &[u8]) -> Hmac<Sha384> {
        let mut mac =
            <Hmac<Sha384> as KeyInit>::new_from_slice(&self.secret).expect("48-byte HMAC key");
        mac.update(prefix.as_bytes());
        mac.update(body);
        mac
    }
}

#[derive(Default)]
struct Replay {
    clock: u64,
    nonces: HashMap<[u8; 32], (u64, Instant)>,
}

pub(super) struct Receiver {
    pub(super) link: Link,
    pub(super) epoch: String,
    keys: Keys,
    replay: Mutex<Replay>,
}

pub(super) enum Authentication {
    Invalid,
    StaleEpoch,
    Reused,
    Full,
}

impl Receiver {
    pub(super) fn new(link: Link, keys: Keys) -> Result<Self> {
        Ok(Self {
            link,
            epoch: encode(&random()?),
            keys,
            replay: Mutex::default(),
        })
    }

    pub(super) fn authenticate(
        &self,
        id: &str,
        envelope: &Envelope<'_>,
        body: &[u8],
        signature: &str,
    ) -> std::result::Result<&Key, Authentication> {
        let time = unix_seconds().map_err(|_| Authentication::Invalid)?;
        self.authenticate_at(id, envelope, body, signature, time, Instant::now())
    }

    fn authenticate_at(
        &self,
        id: &str,
        envelope: &Envelope<'_>,
        body: &[u8],
        signature: &str,
        wall: u64,
        instant: Instant,
    ) -> std::result::Result<&Key, Authentication> {
        use Authentication::{Full, Invalid, Reused, StaleEpoch};
        let timestamp = decimal(envelope.timestamp).map_err(|_| Invalid)?;
        let nonce = decode(envelope.nonce).map_err(|_| Invalid)?;
        let tag: [u8; 48] = decode(signature).map_err(|_| Invalid)?;
        let mut replay = self.replay.lock().map_err(|_| Full)?;
        replay.clock = replay.clock.max(wall);
        let now = replay.clock;
        if now.abs_diff(timestamp) > SKEW {
            return Err(Invalid);
        }
        let key = self.keys.select(id, now).ok_or(Invalid)?;
        key.request(self.link, envelope, body)
            .verify_slice(&tag)
            .map_err(|_| Invalid)?;
        if envelope.route == Route::Identify {
            if envelope.epoch != "bootstrap" {
                return Err(Invalid);
            }
        } else if envelope.epoch != self.epoch {
            return Err(StaleEpoch);
        }
        if replay.nonces.len() == NONCES {
            replay.nonces.retain(|_, (signed, used)| {
                now.saturating_sub(*signed) <= SKEW
                    || instant.duration_since(*used).as_secs() < NONCE_SECONDS
            });
        }
        if replay.nonces.contains_key(&nonce) {
            return Err(Reused);
        }
        if replay.nonces.len() == NONCES {
            return Err(Full);
        }
        replay.nonces.insert(nonce, (timestamp, instant));
        Ok(key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn fixture_key() -> Key {
        Key {
            id: "vector".into(),
            secret: std::array::from_fn(|index| index as u8),
        }
    }

    #[test]
    fn canonical_request_and_response_match_golden_vectors() {
        let key = fixture_key();
        let nonce = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8";
        let envelope = Envelope {
            epoch: "bootstrap",
            timestamp: "1700000000",
            nonce,
            route: Route::Identify,
        };
        let body = b"{ \"probe\": true }";
        let request_tag =
            decode::<48>("6R4NbclJRJuBP8PIE45bHElxJQC5ONT6OPkMdRcPrPH68eR5FGEb_BiEUyl7i5mU")
                .unwrap();
        let response_tag =
            decode::<48>("tqoREO5TH9OwUPVdI5TQRGGG3GuNctWyWPO7h1n2pRB4yDA06FBwUhDhVPk5d3Hg")
                .unwrap();
        assert!(
            key.request(Link::FeedSync, &envelope, body)
                .verify_slice(&request_tag)
                .is_ok()
        );
        assert!(
            key.response(Link::FeedSync, nonce, 200, body)
                .verify_slice(&response_tag)
                .is_ok()
        );
        assert!(
            key.request(Link::FeedSync, &envelope, b"{ \"probe\": truE }")
                .verify_slice(&request_tag)
                .is_err()
        );
        assert!(
            key.request(Link::FeedSync, &envelope, b"{\"probe\":true}")
                .verify_slice(&request_tag)
                .is_err()
        );
        assert!(
            key.request(Link::SyncServe, &envelope, body)
                .verify_slice(&request_tag)
                .is_err()
        );
        assert!(
            key.response(Link::FeedSync, nonce, 202, body)
                .verify_slice(&response_tag)
                .is_err()
        );
        assert!(
            key.response(Link::SyncServe, nonce, 200, body)
                .verify_slice(&response_tag)
                .is_err()
        );
    }

    #[test]
    fn replay_cache_is_atomic_and_fail_closed() {
        let receiver = Receiver::new(
            Link::FeedSync,
            Keys {
                active: fixture_key(),
                previous: None,
            },
        )
        .unwrap();
        let instant = Instant::now();
        let attempt = |id: u64, timestamp: &str, wall, time| {
            let mut bytes = [0; 32];
            bytes[..8].copy_from_slice(&id.to_be_bytes());
            let nonce = encode(&bytes);
            let envelope = Envelope {
                epoch: &receiver.epoch,
                timestamp,
                nonce: &nonce,
                route: Route::Sync,
            };
            let signature = encode(
                &fixture_key()
                    .request(Link::FeedSync, &envelope, b"{}")
                    .finalize()
                    .into_bytes(),
            );
            receiver.authenticate_at("vector", &envelope, b"{}", &signature, wall, time)
        };
        std::thread::scope(|scope| {
            let threads: Vec<_> = (0..16)
                .map(|_| scope.spawn(|| attempt(0, "1000", 1000, instant).is_ok()))
                .collect();
            assert_eq!(
                threads
                    .into_iter()
                    .map(|thread| thread.join().unwrap() as usize)
                    .sum::<usize>(),
                1
            );
        });
        for id in 1..NONCES as u64 {
            assert!(attempt(id, "1000", 1000, instant).is_ok());
        }
        assert!(matches!(
            attempt(10_000, "1000", 1000, instant),
            Err(Authentication::Full)
        ));
        assert!(attempt(10_000, "1150", 1150, instant + Duration::from_secs(150)).is_ok());
        assert!(matches!(
            attempt(0, "1000", 1000, instant + Duration::from_secs(151)),
            Err(Authentication::Invalid)
        ));
    }

    #[test]
    fn epochs_rotation_and_timestamp_edges() {
        let mut previous = fixture_key();
        previous.id = "previous".into();
        let receiver = Receiver::new(
            Link::FeedSync,
            Keys {
                active: fixture_key(),
                previous: Some((previous, 1000)),
            },
        )
        .unwrap();
        let signed = |key: &Key, epoch, timestamp, route, byte| {
            let nonce = encode(&[byte; 32]);
            let envelope = Envelope {
                epoch,
                timestamp,
                nonce: &nonce,
                route,
            };
            let tag = encode(
                &key.request(Link::FeedSync, &envelope, b"{}")
                    .finalize()
                    .into_bytes(),
            );
            receiver.authenticate_at(
                &key.id,
                &envelope,
                b"{}",
                &tag,
                999 + u64::from(byte > 1),
                Instant::now(),
            )
        };
        let previous = &receiver.keys.previous.as_ref().unwrap().0;
        assert!(signed(previous, "bootstrap", "939", Route::Identify, 1).is_ok());
        assert!(matches!(
            signed(previous, "bootstrap", "1000", Route::Identify, 2),
            Err(Authentication::Invalid)
        ));
        assert!(matches!(
            signed(&receiver.keys.active, "bootstrap", "1000", Route::Sync, 3),
            Err(Authentication::StaleEpoch)
        ));
        assert!(matches!(
            signed(
                &receiver.keys.active,
                "bootstrap",
                "1061",
                Route::Identify,
                4
            ),
            Err(Authentication::Invalid)
        ));
        assert!(matches!(
            signed(
                &receiver.keys.active,
                "bootstrap",
                "18446744073709551615",
                Route::Identify,
                5
            ),
            Err(Authentication::Invalid)
        ));
        assert!(matches!(
            signed(
                &receiver.keys.active,
                &receiver.epoch,
                "1000",
                Route::Identify,
                6
            ),
            Err(Authentication::Invalid)
        ));
        let old = Receiver::new(
            Link::FeedSync,
            Keys {
                active: fixture_key(),
                previous: None,
            },
        )
        .unwrap();
        assert!(matches!(
            signed(&receiver.keys.active, &old.epoch, "1000", Route::Sync, 7),
            Err(Authentication::StaleEpoch)
        ));
        assert!(
            signed(
                &receiver.keys.active,
                &receiver.epoch,
                "1060",
                Route::Sync,
                8
            )
            .is_ok()
        );
        assert!(decode::<48>("broken=").is_err());
        assert!(decimal("01000").is_err());
    }
}
