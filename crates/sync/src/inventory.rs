//! Bounded interpretation of the packaged rsync client's list-only output.

use crate::{
    shutdown::Shutdown,
    upstream::{MAX_CATALOG_ENTRIES, ProcessGroup, TXT_LIMIT, filename},
    xml,
};
use rfc_core::RfcNumber;
use rfc_storage::postgres::Result;
use serde::Serialize;
use std::{collections::BTreeMap, fmt, process::Stdio, time::Duration};
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    process::Command,
    time::Instant,
};

const STDOUT_LIMIT: u64 = 8 * 1024 * 1024;
const STDERR_LIMIT: u64 = 64 * 1024;
pub(crate) const OUTPUT_RESERVATION: u64 = STDOUT_LIMIT + STDERR_LIMIT;
const LINE_LIMIT: usize = 256;
const PREVIEW_LIMIT: usize = 96;
const ANCILLARY_LINK: &str = "rfc-ref.txt";
const ANCILLARY_TARGET: &str = "rfc-ref.txt.new";

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Entry {
    pub length: u64,
    pub modified_utc: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum Rejection {
    TruncatedListing,
    EntryLimit,
    LineLimit,
    InvalidCharacters,
    MalformedRow,
    InvalidSize,
    InvalidMetadata,
    InvalidRoot,
    InvalidPath,
    UnexpectedLink,
    UnexpectedFileKind,
    UnexpectedFilename,
    RfcSizeLimit,
    DuplicateRfc,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct ParseError {
    reason: Rejection,
    line: usize,
    entry_preview: String,
}

impl ParseError {
    fn new(reason: Rejection, line: usize, entry: &[u8]) -> Self {
        let mut entry_preview = String::new();
        for &byte in entry {
            let escaped = std::ascii::escape_default(byte);
            if entry_preview.len() + escaped.len() > PREVIEW_LIMIT {
                break;
            }
            entry_preview.extend(escaped.map(char::from));
        }
        Self {
            reason,
            line,
            entry_preview,
        }
    }
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self.reason {
            Rejection::TruncatedListing => "truncated rsync inventory listing",
            Rejection::EntryLimit => "rsync inventory exceeds entry limit",
            Rejection::LineLimit => "rsync inventory exceeds line limit",
            Rejection::InvalidCharacters => "rsync inventory contains invalid characters",
            Rejection::MalformedRow => "malformed rsync inventory row",
            Rejection::InvalidSize => "invalid rsync inventory size",
            Rejection::InvalidMetadata => "invalid rsync inventory metadata",
            Rejection::InvalidRoot => "invalid rsync inventory root",
            Rejection::InvalidPath => "rsync inventory path is not a canonical root basename",
            Rejection::UnexpectedLink => "unexpected rsync inventory symbolic link",
            Rejection::UnexpectedFileKind => "rsync inventory entry is not a regular file",
            Rejection::UnexpectedFilename => "unexpected or noncanonical rsync inventory filename",
            Rejection::RfcSizeLimit => "rsync inventory RFC TXT size exceeds bounds",
            Rejection::DuplicateRfc => "duplicate RFC in rsync inventory",
        };
        write!(
            f,
            "{message} at line {}: \"{}\"",
            self.line, self.entry_preview
        )
    }
}

impl std::error::Error for ParseError {}

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
            Ok(parse(&stdout)?)
        }
        Err(error) => {
            group.kill();
            child.wait().await?;
            Err(error)
        }
    }
}

pub(crate) async fn read_bounded(input: impl AsyncRead + Unpin, limit: u64) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    bytes.try_reserve_exact(usize::try_from(limit + 1)?)?;
    input.take(limit + 1).read_to_end(&mut bytes).await?;
    if bytes.len() as u64 > limit {
        return Err("rsync inventory output exceeds byte limit".into());
    }
    Ok(bytes)
}

fn parse(bytes: &[u8]) -> std::result::Result<BTreeMap<RfcNumber, Entry>, ParseError> {
    if bytes.is_empty() {
        return Err(ParseError::new(Rejection::InvalidRoot, 1, bytes));
    }
    let mut entries = BTreeMap::new();
    for (index, line) in bytes.split_inclusive(|&byte| byte == b'\n').enumerate() {
        let row = line
            .strip_suffix(b"\n")
            .ok_or_else(|| ParseError::new(Rejection::TruncatedListing, index + 1, line))?;
        let row_error = |reason| ParseError::new(reason, index + 1, row);
        if index > MAX_CATALOG_ENTRIES {
            return Err(row_error(Rejection::EntryLimit));
        }
        if row.len() > LINE_LIMIT {
            return Err(row_error(Rejection::LineLimit));
        }
        if !row
            .iter()
            .all(|byte| byte.is_ascii_graphic() || *byte == b' ')
        {
            return Err(row_error(Rejection::InvalidCharacters));
        }
        let line = std::str::from_utf8(row).map_err(|_| row_error(Rejection::InvalidCharacters))?;
        let mut fields = line.split_ascii_whitespace();
        let [Some(mode), Some(length), Some(date), Some(time), Some(name)] =
            std::array::from_fn(|_| fields.next())
        else {
            return Err(row_error(Rejection::MalformedRow));
        };
        let entry_error = |reason| ParseError::new(reason, index + 1, name.as_bytes());
        if !length.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(entry_error(Rejection::InvalidSize));
        }
        let length: u64 = length
            .parse()
            .map_err(|_| entry_error(Rejection::InvalidSize))?;
        if !valid_mode(mode) || !valid_timestamp(date, time) {
            return Err(entry_error(Rejection::InvalidMetadata));
        }
        if index == 0 || name == "." {
            if index != 0 || name != "." || !mode.starts_with('d') || fields.next().is_some() {
                return Err(entry_error(Rejection::InvalidRoot));
            }
            continue;
        }
        if name.starts_with('.')
            || !name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b".-_".contains(&byte))
        {
            return Err(entry_error(Rejection::InvalidPath));
        }
        if mode.starts_with('l') {
            if name != ANCILLARY_LINK
                || fields.next() != Some("->")
                || fields.next() != Some(ANCILLARY_TARGET)
                || fields.next().is_some()
                || length != ANCILLARY_TARGET.len() as u64
            {
                return Err(row_error(Rejection::UnexpectedLink));
            }
            // The listing reports link metadata only. No link reaches the corpus map.
            continue;
        }
        if fields.next().is_some() {
            return Err(row_error(Rejection::MalformedRow));
        }
        if matches!(name, "bcp" | "fyi" | "std" | "ien") && mode.starts_with('d') {
            continue;
        }
        if !mode.starts_with('-') {
            return Err(entry_error(Rejection::UnexpectedFileKind));
        }
        if is_ancillary(name) {
            continue;
        }
        let id = name
            .strip_prefix("rfc")
            .and_then(|stem| stem.strip_suffix(".txt"))
            .and_then(xml::number)
            .filter(|&id| filename(id) == name)
            .ok_or_else(|| entry_error(Rejection::UnexpectedFilename))?;
        if !(1..=TXT_LIMIT as u64).contains(&length) {
            return Err(entry_error(Rejection::RfcSizeLimit));
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
            return Err(entry_error(Rejection::DuplicateRfc));
        }
    }
    Ok(entries)
}

fn is_ancillary(name: &str) -> bool {
    matches!(
        name,
        "RFCs_for_errata.txt"
            | "bcp-index.txt"
            | "bcp-ref.txt"
            | "fyi-index.txt"
            | "ien-index.txt"
            | "rfc-index-latest.txt"
            | "rfc-index.txt"
            | "rfc-ref-old.txt"
            | ANCILLARY_LINK
            | "rfc17a.txt"
            | "rfcxx00.txt"
            | "std-index.txt"
            | "std-ref.txt"
            | "README"
            | "README.txt"
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
    b"-dl".contains(&kind)
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
    fn component(text: &[u8]) -> Option<u16> {
        if !text.iter().all(u8::is_ascii_digit) {
            return None;
        }
        std::str::from_utf8(text).ok()?.parse().ok()
    }
    let date = date.as_bytes();
    let time = time.as_bytes();
    if date.len() != 10
        || time.len() != 8
        || date[4] != b'/'
        || date[7] != b'/'
        || time[2] != b':'
        || time[5] != b':'
    {
        return false;
    }
    let [
        Some(year),
        Some(month),
        Some(day),
        Some(hour),
        Some(minute),
        Some(second),
    ] = [
        component(&date[..4]),
        component(&date[5..7]),
        component(&date[8..]),
        component(&time[..2]),
        component(&time[3..5]),
        component(&time[6..]),
    ]
    else {
        return false;
    };
    let leap = year.is_multiple_of(4) && (!year.is_multiple_of(100) || year.is_multiple_of(400));
    let days = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => 28 + u16::from(leap),
        _ => return false,
    };
    year > 0 && (1..=days).contains(&day) && hour <= 23 && minute <= 59 && second <= 60
}

#[cfg(test)]
mod tests {
    use super::*;

    const ROOT: &str = "drwxr-xr-x             80 2026/11/24 16:21:43 .\n";
    const REGULAR: &str = "-rw-r--r--              5 2026/11/24 16:21:43 ";
    const LINK: &str = "lrwxrwxrwx             15 2026/11/24 16:21:43 ";

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
    fn complete_rfc_editor_inventory_preserves_canonical_metadata() {
        let bytes = include_bytes!("../../../fixtures/rfc/rsync-inventory-2026-11-25.txt");
        let inventory = parse(bytes).unwrap();
        assert_eq!(inventory.len(), 9862);
        let rows = std::str::from_utf8(bytes).unwrap().lines();
        assert_eq!(rows.clone().count(), 9880);
        for row in rows {
            let mut fields = row.split_ascii_whitespace();
            let (mode, length, date, time, name) = (
                fields.next().unwrap(),
                fields.next().unwrap(),
                fields.next().unwrap(),
                fields.next().unwrap(),
                fields.next().unwrap(),
            );
            if mode.starts_with('-')
                && let Some(number) = name
                    .strip_prefix("rfc")
                    .and_then(|name| name.strip_suffix(".txt"))
                && let Ok(number) = number.parse()
            {
                let id = RfcNumber::new(number).unwrap();
                assert_eq!(
                    inventory[&id],
                    Entry {
                        length: length.parse().unwrap(),
                        modified_utc: format!("{date} {time}"),
                    },
                    "{name}",
                );
            }
        }
    }

    #[test]
    fn accepts_only_the_known_ancillary_link() {
        let inventory = parse(
            format!("{ROOT}{LINK}rfc-ref.txt -> rfc-ref.txt.new\n{REGULAR}rfc1.txt\n").as_bytes(),
        )
        .unwrap();
        assert_eq!(inventory.keys().map(|id| id.get()).collect::<Vec<_>>(), [1]);

        for name in [
            "rfc1.txt -> rfc-ref.txt.new",
            "bcp-index.txt -> rfc-ref.txt.new",
            "rfc-ref.txt -> rfc1.txt",
            "rfc-ref.txt -> ../rfc-ref.txt.new",
            "rfc-ref.txt -> /rfc-ref.txt.new",
            "rfc-ref.txt -> rfc-ref.txt.new extra",
            "rfc-ref.txt",
        ] {
            let error = parse(format!("{ROOT}{LINK}{name}\n").as_bytes()).unwrap_err();
            assert_eq!(error.reason, Rejection::UnexpectedLink, "{name}");
            assert_eq!(error.line, 2);
            assert!(error.entry_preview.ends_with(name), "{error}");
        }
        let wrong_size = LINK.replace("15", "14");
        let error = parse(format!("{ROOT}{wrong_size}rfc-ref.txt -> rfc-ref.txt.new\n").as_bytes())
            .unwrap_err();
        assert_eq!(error.reason, Rejection::UnexpectedLink);
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
            format!("{ROOT}-rw-r--r-- 5 2026/02/29 16:21:43 rfc1.txt\n"),
            format!("{ROOT}{REGULAR}{}.txt\n", "a".repeat(LINE_LIMIT)),
            format!("{ROOT}-rw-r--r-- 0 2026/11/24 16:21:43 rfc1.txt\n"),
            format!("{ROOT}prw-r--r-- 0 2026/11/24 16:21:43 bcp-index.txt\n"),
            format!("{ROOT}crw-r--r-- 0 2026/11/24 16:21:43 rfc1.txt\n"),
            format!("{ROOT}-zw-r--r-- 5 2026/11/24 16:21:43 rfc1.txt\n"),
            format!("{ROOT}{REGULAR}rfc18a.txt\n"),
            format!("{ROOT}{REGULAR}rfc-ref.txt.new\n"),
        ] {
            assert!(parse(bad.as_bytes()).is_err());
        }
    }

    #[test]
    fn escapes_and_bounds_rejection_diagnostics() {
        let row = [b"invalid\"\\\r\x1b\xff ".as_slice(), &[b'x'; 110]].concat();
        let bytes = [ROOT.as_bytes(), &row, b"\n"].concat();
        let error = parse(&bytes).unwrap_err();
        let report = serde_json::to_value(&error).unwrap();
        assert_eq!(report["reason"], "invalid_characters");
        assert_eq!(report["line"], 2);
        assert!(error.entry_preview.starts_with(r#"invalid\"\\\r\x1b\xff "#));
        assert_eq!(error.entry_preview.len(), PREVIEW_LIMIT);
        assert!(!error.to_string().chars().any(char::is_control));
    }

    #[test]
    fn ancillary_rows_count_towards_inventory_entry_limit() {
        let listing = format!(
            "{ROOT}{}",
            format!("{REGULAR}README\n").repeat(MAX_CATALOG_ENTRIES + 1)
        );
        let error = parse(listing.as_bytes()).unwrap_err();
        assert_eq!(error.reason, Rejection::EntryLimit);
        assert_eq!(error.line, MAX_CATALOG_ENTRIES + 2);
    }

    #[tokio::test]
    async fn enforces_output_limit() {
        assert_eq!(read_bounded(&b"1234"[..], 4).await.unwrap(), b"1234");
        assert!(read_bounded(&b"12345"[..], 4).await.is_err());
    }
}
