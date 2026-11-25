//! Rsync audit and one-file repair. Compare output is accepted only as a whole.

use super::{BatchReceipt, Input};
use crate::{
    inventory::read_bounded,
    metrics::Stage,
    rsync::{RSYNC, check, child_rules},
    shutdown::Shutdown,
    upstream::{ProcessGroup, TXT_LIMIT, Upstream, now_us},
};
use rfc_core::BlobDigest;
use rfc_storage::{
    blob::BlobReader,
    postgres::{Result, mirror_root},
};
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    os::{
        fd::AsRawFd,
        unix::fs::{MetadataExt, OpenOptionsExt},
    },
    path::Path,
    process::Stdio,
    time::Duration,
};
use tokio::{process::Command, time::Instant};

const FORMAT: &str = "--out-format=RFC|%i|%l|%n";
const OUTPUT_LIMIT: u64 = 128 * 1024;

pub(super) fn open_regular(path: &Path) -> io::Result<File> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)?;
    let meta = file.metadata()?;
    if !meta.is_file() || meta.nlink() != 1 {
        return Err(io::Error::other(
            "audit path is not an independent regular file",
        ));
    }
    Ok(file)
}

fn hash(
    mut file: File,
    expected: u64,
    mut destination: Option<File>,
    stop: &Shutdown,
) -> Result<BlobDigest> {
    let mut hasher = blake3::Hasher::new();
    let mut buf = [0; 64 * 1024];
    let mut remaining = expected;
    if file.metadata()?.len() != expected {
        return Err("audit snapshot has an unexpected length".into());
    }
    while remaining > 0 {
        stop.check()?;
        let size = remaining.min(buf.len() as u64) as usize;
        file.read_exact(&mut buf[..size])?;
        hasher.update(&buf[..size]);
        if let Some(output) = &mut destination {
            output.write_all(&buf[..size])?;
        }
        remaining -= size as u64;
    }
    if file.read(&mut buf[..1])? != 0 {
        return Err("audit snapshot grew during verification".into());
    }
    Ok(BlobDigest::from_bytes(*hasher.finalize().as_bytes()))
}

pub(super) fn snapshot(path: &Path, inputs: &[Input], stop: &Shutdown) -> Result<()> {
    let canonical = BlobReader::open(&mirror_root(), false)?;
    super::private_dir(&path.join("txt"))?;
    {
        let mut names = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o400)
            .open(path.join("expected"))?;
        for input in inputs {
            let digest = input.digest()?;
            let name = input.filename();
            let file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o400)
                .open(path.join("txt").join(&name))?;
            if hash(canonical.open_blob(digest)?, input.length, Some(file), stop)? != digest {
                return Err(
                    format!("RFC {} canonical TXT failed audit verification", input.rfc).into(),
                );
            }
            writeln!(names, "{name}")?;
        }
    }
    Ok(())
}

pub(super) fn verify(path: &Path, inputs: &[Input], stop: &Shutdown) -> Result<()> {
    let snapshot = path.join("txt");
    if fs::read_dir(&snapshot)?.count() != inputs.len() {
        return Err("rsync audit snapshot membership changed".into());
    }
    for input in inputs {
        let file = open_regular(&snapshot.join(input.filename()))?;
        if file.metadata()?.mode() & 0o7777 != 0o400
            || hash(file, input.length, None, stop)? != input.digest()?
        {
            return Err(format!("RFC {} rsync audit snapshot changed", input.rfc).into());
        }
    }
    Ok(())
}

pub(super) async fn compare(
    path: &Path,
    inputs: &[Input],
    upstream: &Upstream,
    stop: &Shutdown,
    timeout: Duration,
) -> Result<(BatchReceipt, BTreeMap<u32, bool>)> {
    let started_us = now_us()?;
    let mut command = command(path, false)?;
    command
        .args([
            "--dry-run",
            "--checksum",
            "--checksum-choice=auto",
            "-ii",
            "--times",
            "--links",
            "--specials",
            "--devices",
            "--debug=NSTR1",
            FORMAT,
            "--files-from=expected",
            "--",
        ])
        .arg(upstream.rsync_module())
        .arg("txt/");
    let output = run(command, upstream, stop, timeout).await?;
    let (algorithm, decisions) = parse_compare(&output, inputs)?;
    let receipt = BatchReceipt {
        id: BlobDigest::hash(&serde_json::to_vec(&(started_us, inputs, &output))?).to_string(),
        started_us,
        observed_us: now_us()?,
        checksum_choice: "auto".into(),
        algorithm,
        module: upstream.rsync_module().into(),
    };
    Ok((receipt, decisions))
}

fn parse_compare(output: &[u8], inputs: &[Input]) -> Result<(Option<String>, BTreeMap<u32, bool>)> {
    let expected: BTreeMap<_, _> = inputs
        .iter()
        .map(|input| (input.filename(), input))
        .collect();
    let mut algorithm = None;
    let mut results = BTreeMap::new();
    for line in lines(output)? {
        // For this pinned client a single auto choice sets the pre-transfer
        // and transfer algorithms. The daemon-auth diagnostic is a separate sum.
        if let Some(name) = line
            .strip_prefix("Client negotiated checksum: ")
            .or_else(|| line.strip_prefix("Client checksum: "))
        {
            if !matches!(name, "xxh128" | "xxh3" | "xxh64" | "md5" | "md4" | "sha1")
                || algorithm.replace(name.to_owned()).is_some()
            {
                return Err("disabled, unknown or duplicate rsync checksum negotiation".into());
            }
            continue;
        }
        let (flags, length, name) = row(line)?;
        let input = expected
            .get(name)
            .ok_or("unexpected filename in rsync audit receipt")?;
        let matched = match flags.as_bytes() {
            [
                b'.',
                b'f',
                b' ',
                b' ',
                b' ',
                b' ',
                b' ',
                b' ',
                b' ',
                b' ',
                b' ',
            ]
            | [
                b'.',
                b'f',
                b'.',
                b'.',
                b't',
                b'.',
                b'.',
                b'.',
                b'.',
                b'.',
                b'.',
            ] if length == input.length => true,
            [
                b'>',
                b'f',
                b'c',
                b'.' | b's',
                b'.' | b't',
                b'.',
                b'.',
                b'.',
                b'.',
                b'.',
                b'.',
            ] if (flags.as_bytes()[3] == b's') == (length != input.length) => false,
            _ => return Err("invalid file kind or itemization in rsync audit receipt".into()),
        };
        if results.insert(input.rfc, matched).is_some() {
            return Err("duplicate file in rsync audit receipt".into());
        }
    }
    if results.len() != expected.len() {
        return Err("rsync audit receipt omits expected files".into());
    }
    Ok((algorithm, results))
}

pub(super) async fn fetch(
    path: &Path,
    input: &Input,
    upstream: &Upstream,
    stop: &Shutdown,
    timeout: Duration,
) -> Result<i64> {
    let name = input.filename();
    let mut command = command(path, true)?;
    command
        .args([
            "--whole-file",
            "--max-size=4194304",
            "--min-size=1",
            "-ii",
            FORMAT,
            "--",
        ])
        .arg(format!("{}{name}", upstream.rsync_module()))
        .arg("./");
    let output = run(command, upstream, stop, timeout).await?;
    let mut receipt = lines(&output)?;
    let (flags, len, received) = row(receipt.next().ok_or("rsync repair receipt is empty")?)?;
    if flags != ">f+++++++++"
        || received != name
        || receipt.next().is_some()
        || !(1..=TXT_LIMIT as u64).contains(&len)
        || open_regular(&path.join(name))?.metadata()?.len() != len
        || fs::read_dir(path)?.count() != 1
    {
        return Err("rsync repair receipt or inventory is incomplete".into());
    }
    upstream.metrics().staged(len, len, None, 1, 0);
    now_us()
}

fn lines(bytes: &[u8]) -> Result<impl Iterator<Item = &str>> {
    if !bytes.ends_with(b"\n") || !bytes.iter().all(|&b| b == b'\n' || (32..=126).contains(&b)) {
        return Err("invalid or truncated rsync output".into());
    }
    Ok(std::str::from_utf8(bytes)?.split_terminator('\n'))
}

fn row(line: &str) -> Result<(&str, u64, &str)> {
    let mut fields = line.split('|');
    let [Some("RFC"), Some(flags), Some(length), Some(name), None] =
        std::array::from_fn(|_| fields.next())
    else {
        return Err("unrecognized rsync output".into());
    };
    if flags.len() != 11 || length.is_empty() || !length.bytes().all(|b| b.is_ascii_digit()) {
        return Err("malformed rsync receipt row".into());
    }
    Ok((flags, length.parse()?, name))
}

fn command(path: &Path, writable: bool) -> Result<Command> {
    let rules = child_rules(path, writable)?;
    let parent = std::process::id() as i32;
    let mut cmd = Command::new(RSYNC);
    cmd.current_dir(path)
        .env_clear()
        .env("LC_ALL", "C")
        .env("TZ", "UTC")
        .args([
            "--no-motd",
            "--no-human-readable",
            "--contimeout=5",
            "--timeout=30",
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .process_group(0)
        .kill_on_drop(true);
    // Everything in pre_exec is an async-signal-safe syscall. The process group
    // is killed and reaped on timeout or cancellation, and tied to its parent.
    unsafe {
        cmd.pre_exec(move || {
            check(libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL, 0, 0, 0).into())?;
            if libc::getppid() != parent {
                return Err(io::Error::other("cannot supervise audit rsync child"));
            }
            let address = libc::rlimit {
                rlim_cur: 256 * 1024 * 1024,
                rlim_max: 256 * 1024 * 1024,
            };
            let file = libc::rlimit {
                rlim_cur: TXT_LIMIT as _,
                rlim_max: TXT_LIMIT as _,
            };
            check(libc::setrlimit(libc::RLIMIT_AS, &address).into())?;
            check(libc::setrlimit(libc::RLIMIT_FSIZE, &file).into())?;
            check(libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0).into())?;
            check(libc::syscall(
                libc::SYS_landlock_restrict_self,
                rules.as_raw_fd(),
                0_u32,
            ))?;
            Ok(())
        });
    }
    Ok(cmd)
}

async fn run(
    mut command: Command,
    upstream: &Upstream,
    stop: &Shutdown,
    timeout: Duration,
) -> Result<Vec<u8>> {
    stop.check()?;
    let _metric = upstream.metrics().span(Stage::Rsync);
    let mut child = command.spawn()?;
    let mut group = ProcessGroup(child.id().map(i32::try_from).transpose()?);
    let stdout = child.stdout.take().ok_or("audit stdout unavailable")?;
    let stderr = child.stderr.take().ok_or("audit stderr unavailable")?;
    let result = stop
        .until(Instant::now() + timeout, async {
            let (out, err, status) = tokio::try_join!(
                read_bounded(stdout, OUTPUT_LIMIT),
                read_bounded(stderr, 64 * 1024),
                async { Ok(child.wait().await?) },
            )?;
            if !status.success() || !err.is_empty() {
                return Err(format!("rsync audit or repair session failed ({status})").into());
            }
            Ok(out)
        })
        .await;
    match result {
        Ok(out) => {
            group.0 = None;
            Ok(out)
        }
        Err(error) => {
            group.kill();
            child.wait().await?;
            Err(error)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn inputs() -> Vec<Input> {
        [100, 101, 102, 103]
            .map(|rfc| Input {
                rfc,
                length: 2,
                digest: String::new(),
                observation: String::new(),
            })
            .into()
    }

    #[test]
    fn requires_explicit_rows_and_distinguishes_checksum_from_mtime() {
        let good = b"Client negotiated checksum: xxh128\nRFC|.f         |2|rfc100.txt\nRFC|>fc........|2|rfc101.txt\nRFC|>fcs.......|3|rfc102.txt\nRFC|.f..t......|2|rfc103.txt\n";
        let (algorithm, outcomes) = parse_compare(good, &inputs()).unwrap();
        assert_eq!(algorithm.as_deref(), Some("xxh128"));
        assert_eq!(
            outcomes.into_values().collect::<Vec<_>>(),
            [true, false, false, true]
        );
        let text = std::str::from_utf8(good).unwrap();
        let legacy = text.replace("Client negotiated checksum: xxh128", "Client checksum: md4");
        assert_eq!(
            parse_compare(legacy.as_bytes(), &inputs())
                .unwrap()
                .0
                .as_deref(),
            Some("md4")
        );
        for bad in [
            text.replace("xxh128", "none"),
            text.replace("rfc101.txt", "rfc100.txt"),
            text.replace("rfc101.txt", "rfc00101.txt"),
            text.replace(">fc........", "cL+++++++++"),
            text.replace(">fcs.......|3", ">fcs.......|2"),
            text.trim_end().to_string(),
            format!("{text}extra\n"),
            text.lines().take(4).collect::<Vec<_>>().join("\n") + "\n",
        ] {
            assert!(parse_compare(bad.as_bytes(), &inputs()).is_err(), "{bad}");
        }
    }

    #[tokio::test]
    async fn rejects_output_beyond_the_receipt_limit() {
        let output = vec![b'x'; OUTPUT_LIMIT as usize + 1];
        assert!(read_bounded(output.as_slice(), OUTPUT_LIMIT).await.is_err());
    }
}
