//! Bounded interpretation of the packaged rsync client's list-only output.

use crate::{
    shutdown::Shutdown,
    upstream::{MAX_CATALOG_ENTRIES, ProcessGroup, TXT_LIMIT, filename},
    xml,
};
use rfc_core::RfcNumber;
use rfc_storage::postgres::Result;
use std::{collections::BTreeMap, process::Stdio, time::Duration};
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    process::Command,
    time::Instant,
};

const STDOUT_LIMIT: u64 = 8 * 1024 * 1024;
const STDERR_LIMIT: u64 = 64 * 1024;
pub(crate) const OUTPUT_RESERVATION: u64 = STDOUT_LIMIT + STDERR_LIMIT;
const LINE_LIMIT: usize = 256;

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Entry {
    pub length: u64,
    pub modified_utc: String,
}

pub(crate) async fn list(
    module: &str,
    timeout: Duration,
    shutdown: &Shutdown,
) -> Result<BTreeMap<RfcNumber, Entry>> {
    shutdown.check()?;
    let deadline = Instant::now() + timeout;
    let mut command = Command::new("/usr/bin/rsync");
    command
        .env_clear()
        .env("LC_ALL", "C")
        .env("TZ", "UTC")
        .args([
            "--list-only",
            "--dirs",
            "--links",
            "--specials",
            "--devices",
            "--no-human-readable",
            "--no-motd",
            "--contimeout=5",
            "--timeout=30",
            "--",
        ])
        .arg(module)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .process_group(0)
        .kill_on_drop(true);
    // Rsync may receive its file list before writing stdout. Bound its address
    // space independently of the pipe limits with an async-signal-safe call.
    unsafe {
        command.pre_exec(|| {
            let limit = libc::rlimit {
                rlim_cur: 256 * 1024 * 1024,
                rlim_max: 256 * 1024 * 1024,
            };
            if libc::setrlimit(libc::RLIMIT_AS, &limit) != 0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let mut child = command.spawn()?;
    let mut group = ProcessGroup(child.id().map(i32::try_from).transpose()?);
    let stdout = child
        .stdout
        .take()
        .ok_or("rsync inventory stdout is unavailable")?;
    let stderr = child
        .stderr
        .take()
        .ok_or("rsync inventory stderr is unavailable")?;
    let output = shutdown
        .until(deadline, async {
            let (stdout, _) = tokio::try_join!(
                read_bounded(stdout, STDOUT_LIMIT),
                read_bounded(stderr, STDERR_LIMIT)
            )?;
            Ok((child.wait().await?, stdout))
        })
        .await;
    match output {
        Ok((status, stdout)) => {
            if !status.success() {
                group.kill();
                return Err(format!("rsync inventory failed ({status})").into());
            }
            group.0 = None;
            parse(&stdout)
        }
        Err(error) => {
            group.kill();
            child.wait().await?;
            Err(error)
        }
    }
}

async fn read_bounded(input: impl AsyncRead + Unpin, limit: u64) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    bytes.try_reserve_exact(usize::try_from(limit + 1)?)?;
    input.take(limit + 1).read_to_end(&mut bytes).await?;
    if bytes.len() as u64 > limit {
        return Err("rsync inventory output exceeds byte limit".into());
    }
    Ok(bytes)
}

fn parse(bytes: &[u8]) -> Result<BTreeMap<RfcNumber, Entry>> {
    let input = std::str::from_utf8(bytes)?;
    if !bytes.ends_with(b"\n") {
        return Err("rsync inventory has a truncated listing".into());
    }
    let mut root = false;
    let mut entries = BTreeMap::new();
    for (index, line) in input.split_terminator('\n').enumerate() {
        if index > MAX_CATALOG_ENTRIES
            || line.len() > LINE_LIMIT
            || !line
                .bytes()
                .all(|byte| byte.is_ascii_graphic() || byte == b' ')
        {
            return Err("rsync inventory exceeds entry bounds or has invalid characters".into());
        }
        let mut fields = line.split_ascii_whitespace();
        let [
            Some(mode),
            Some(length),
            Some(date),
            Some(time),
            Some(name),
            None,
        ] = std::array::from_fn(|_| fields.next())
        else {
            return Err("malformed rsync inventory row".into());
        };
        if !length.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err("invalid rsync inventory size".into());
        }
        let length: u64 = length.parse().map_err(|_| "invalid rsync inventory size")?;
        if !valid_mode(mode) || !valid_timestamp(date, time) {
            return Err("invalid rsync inventory metadata".into());
        }
        if name == "." {
            if index != 0 || !mode.starts_with('d') {
                return Err("invalid rsync inventory root".into());
            }
            root = true;
            continue;
        }
        if !root
            || name.starts_with('.')
            || !name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b".-_".contains(&byte))
        {
            return Err("rsync inventory path is not a canonical root basename".into());
        }
        if matches!(name, "bcp" | "fyi" | "std" | "ien") && mode.starts_with('d') {
            continue;
        }
        if !mode.starts_with('-') {
            return Err("rsync inventory entry is not a regular file".into());
        }
        if is_ancillary(name) {
            continue;
        }
        let id = name
            .strip_prefix("rfc")
            .and_then(|stem| stem.strip_suffix(".txt"))
            .and_then(xml::number)
            .filter(|&id| filename(id) == name)
            .ok_or("unexpected or noncanonical rsync inventory filename")?;
        if !(1..=TXT_LIMIT as u64).contains(&length) {
            return Err("rsync inventory RFC TXT size exceeds bounds".into());
        }
        if entries
            .insert(
                id,
                Entry {
                    length,
                    modified_utc: format!("{date} {time}"),
                },
            )
            .is_some()
        {
            return Err("duplicate RFC in rsync inventory".into());
        }
    }
    if !root {
        return Err("rsync inventory root is missing".into());
    }
    Ok(entries)
}

fn is_ancillary(name: &str) -> bool {
    matches!(
        name,
        "rfc-index.txt" | "rfc-ref.txt" | "README" | "README.txt"
    ) || ["bcp", "fyi", "std", "ien"].iter().any(|prefix| {
        name.strip_prefix(prefix)
            .and_then(|stem| stem.strip_suffix(".txt"))
            .and_then(xml::number)
            .is_some()
    })
}

fn valid_mode(mode: &str) -> bool {
    let bytes = mode.as_bytes();
    let [
        kind,
        owner_r,
        owner_w,
        owner_x,
        group_r,
        group_w,
        group_x,
        other_r,
        other_w,
        other_x,
    ] = *bytes
    else {
        return false;
    };
    b"-d".contains(&kind)
        && [owner_r, group_r, other_r]
            .iter()
            .all(|byte| b"r-".contains(byte))
        && [owner_w, group_w, other_w]
            .iter()
            .all(|byte| b"w-".contains(byte))
        && [owner_x, group_x].iter().all(|byte| b"xsS-".contains(byte))
        && b"xtT-".contains(&other_x)
}

fn valid_timestamp(date: &str, time: &str) -> bool {
    fn component(text: &[u8], min: u16, max: u16) -> bool {
        text.iter().all(u8::is_ascii_digit)
            && std::str::from_utf8(text)
                .ok()
                .and_then(|text| text.parse::<u16>().ok())
                .is_some_and(|value| (min..=max).contains(&value))
    }
    date.len() == 10
        && time.len() == 8
        && date.as_bytes()[4] == b'/'
        && date.as_bytes()[7] == b'/'
        && time.as_bytes()[2] == b':'
        && time.as_bytes()[5] == b':'
        && component(&date.as_bytes()[..4], 1, 9999)
        && component(&date.as_bytes()[5..7], 1, 12)
        && component(&date.as_bytes()[8..], 1, 31)
        && component(&time.as_bytes()[..2], 0, 23)
        && component(&time.as_bytes()[3..5], 0, 59)
        && component(&time.as_bytes()[6..], 0, 60)
}

#[cfg(test)]
mod tests {
    use super::*;

    const ROOT: &str = "drwxr-xr-x             80 2026/11/24 16:21:43 .\n";
    const REGULAR: &str = "-rw-r--r--              5 2026/11/24 16:21:43 ";

    #[test]
    fn packaged_listing_selects_only_canonical_rfc_files() {
        let inventory = parse(format!("{ROOT}{REGULAR}rfc1.txt\n{REGULAR}rfc100.txt\n{REGULAR}bcp001.txt\n{REGULAR}rfc-index.txt\n").as_bytes()).unwrap();
        assert_eq!(
            inventory.keys().map(|id| id.get()).collect::<Vec<_>>(),
            [1, 100]
        );
        assert_eq!(inventory[&RfcNumber::new(100).unwrap()].length, 5);
    }

    #[test]
    fn rejects_bad_rows_and_ambiguous_inventory() {
        for name in [
            "../rfc1.txt",
            "rfc0001.txt",
            "RFC1.txt",
            "rfc1.TXT",
            "rfc+1.txt",
            "rfc2147483648.txt",
            "rfc1.txt -> rfc2.txt",
            "rfc-1.txt",
            "rfc1.txt/rfc2.txt",
        ] {
            assert!(
                parse(format!("{ROOT}{REGULAR}{name}\n").as_bytes()).is_err(),
                "{name}"
            );
        }
        for bad in [
            format!("{ROOT}{REGULAR}rfc1.txt\n{REGULAR}rfc1.txt\n"),
            format!("{ROOT}{REGULAR}rfc1.txt"),
            format!("{ROOT}{REGULAR}rfc1.txt\n{ROOT}"),
            format!("{ROOT}drwxr-xr-x 0 2026/11/24 16:21:43 rfc1.txt\n"),
            format!("{ROOT}lrwxrwxrwx 5 2026/11/24 16:21:43 rfc1.txt\n"),
            format!("{ROOT}-rw-r--r-- 18446744073709551616 2026/11/24 16:21:43 rfc1.txt\n"),
            format!("{ROOT}-rw-r--r-- 4194305 2026/11/24 16:21:43 rfc1.txt\n"),
            format!("{ROOT}-rw-r--r-- 5 2026/11/24 25:21:43 rfc1.txt\n"),
            format!("{ROOT}{REGULAR}{}.txt\n", "a".repeat(LINE_LIMIT)),
        ] {
            assert!(parse(bad.as_bytes()).is_err());
        }
    }

    #[tokio::test]
    async fn enforces_output_limit() {
        assert_eq!(read_bounded(&b"1234"[..], 4).await.unwrap(), b"1234");
        assert!(read_bounded(&b"12345"[..], 4).await.is_err());
    }
}
