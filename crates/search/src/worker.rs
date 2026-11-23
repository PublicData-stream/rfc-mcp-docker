//! Supervise the confined worker and validate its JSON against the original.

use std::{fs::File, io, ops::Range, process::Stdio, sync::Arc};

use base64::{Engine, engine::general_purpose::STANDARD};
use serde::Deserialize;
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    process::{Child, Command},
    sync::{OwnedSemaphorePermit, watch},
    time::{Instant, timeout_at},
};

pub(crate) const OUTPUT_BYTES: usize = 8 * 1024 * 1024;
const FRAME_BYTES: usize = 1024 * 1024;
const STDERR_BYTES: u64 = 8 * 1024;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Match {
    pub start: usize,
    pub end: usize,
    pub line: Range<usize>,
}

#[derive(Clone, Copy)]
pub(crate) struct Pattern<'a> {
    pub text: &'a str,
    pub literal: bool,
    pub sensitive: bool,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum WorkerError {
    Sandbox,
    InvalidRegex,
    Unavailable,
    Budget,
    Deadline,
    Cancelled,
    Page,
}

#[derive(Deserialize)]
#[serde(tag = "type", content = "data", rename_all = "snake_case")]
enum Event {
    Begin(serde::de::IgnoredAny),
    Match(Line),
    End(serde::de::IgnoredAny),
    Summary(serde::de::IgnoredAny),
}

#[derive(Deserialize)]
struct Line {
    absolute_offset: usize,
    lines: Text,
    submatches: Vec<Submatch>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum Text {
    Plain { text: String },
    Encoded { bytes: String },
}

impl Text {
    fn into_bytes(self) -> Result<Vec<u8>, WorkerError> {
        match self {
            Self::Plain { text } => Ok(text.into_bytes()),
            Self::Encoded { bytes } => STANDARD.decode(bytes).map_err(|_| WorkerError::Unavailable),
        }
    }
}

#[derive(Deserialize)]
struct Submatch {
    #[serde(rename = "match")]
    content: Text,
    start: usize,
    end: usize,
}

pub(crate) struct Output {
    pub remaining: usize,
    pub matches: Vec<Match>,
    pub after: Option<(usize, usize)>,
    pub wanted: usize,
}

impl Output {
    async fn read(
        &mut self,
        stdout: tokio::process::ChildStdout,
        input: &[u8],
    ) -> Result<bool, WorkerError> {
        let mut reader = BufReader::new(stdout);
        let mut frame = Vec::new();
        let mut summary = false;
        let mut last_line_end = 0;
        while let Ok(bytes) = reader.fill_buf().await {
            if bytes.is_empty() {
                return if frame.is_empty() {
                    Ok(summary)
                } else {
                    Err(WorkerError::Unavailable)
                };
            }
            let newline = memchr::memchr(b'\n', bytes);
            let count = newline.map_or(bytes.len(), |index| index + 1);
            self.remaining = self
                .remaining
                .checked_sub(count)
                .ok_or(WorkerError::Budget)?;
            if frame.len() + count > FRAME_BYTES {
                return Err(WorkerError::Budget);
            }
            frame.extend_from_slice(&bytes[..count]);
            reader.consume(count);
            if newline.is_none() {
                continue;
            }
            if summary {
                return Err(WorkerError::Unavailable);
            }
            let event = serde_json::from_slice(&frame).map_err(|_| WorkerError::Unavailable)?;
            frame.clear();
            match event {
                Event::Begin(_) | Event::End(_) => {}
                Event::Summary(_) => summary = true,
                Event::Match(line) => {
                    let line_end = self.line(line, input, last_line_end)?;
                    last_line_end = line_end;
                }
            }
        }
        Err(WorkerError::Unavailable)
    }

    fn line(
        &mut self,
        line: Line,
        input: &[u8],
        previous_end: usize,
    ) -> Result<usize, WorkerError> {
        let bytes = line.lines.into_bytes()?;
        let start = line.absolute_offset;
        let end = start
            .checked_add(bytes.len())
            .ok_or(WorkerError::Unavailable)?;
        if start < previous_end
            || bytes.is_empty()
            || input.get(start..end) != Some(bytes.as_slice())
            || (start > 0 && input[start - 1] != b'\n')
            || (end < input.len() && bytes.last() != Some(&b'\n'))
            || bytes[..bytes.len() - 1].contains(&b'\n')
            || line.submatches.is_empty()
        {
            return Err(WorkerError::Unavailable);
        }
        let mut previous = None;
        for submatch in line.submatches {
            let pair = (submatch.start, submatch.end);
            if previous.is_some_and(|prior: (usize, usize)| prior >= pair || prior.1 > pair.0)
                || bytes.get(pair.0..pair.1) != Some(submatch.content.into_bytes()?.as_slice())
            {
                return Err(WorkerError::Unavailable);
            }
            previous = Some(pair);
            let absolute = (start + pair.0, start + pair.1);
            if self.after.is_none_or(|after| absolute > after) {
                if self.matches.len() == self.wanted {
                    return Err(WorkerError::Page);
                }
                self.matches.push(Match {
                    start: absolute.0,
                    end: absolute.1,
                    line: start..end,
                });
            }
        }
        Ok(end)
    }
}

type Lease = (File, Arc<OwnedSemaphorePermit>);

struct Process {
    child: Option<Child>,
    lease: Option<Lease>,
}

impl Process {
    fn child(&mut self) -> &mut Child {
        self.child.as_mut().expect("worker has been spawned")
    }

    fn kill(child: &Child) {
        if let Some(id) = child.id() {
            // SAFETY: this is the unreaped process group we created exclusively.
            unsafe { libc::kill(-(id as libc::pid_t), libc::SIGKILL) };
        }
    }

    async fn reap(&mut self) -> Result<std::process::ExitStatus, WorkerError> {
        let status = self
            .child()
            .wait()
            .await
            .map_err(|_| WorkerError::Unavailable)?;
        self.child.take();
        Ok(status)
    }
}

impl Drop for Process {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            Self::kill(&child);
            let lease = self.lease.take();
            tokio::spawn(async move {
                let _lease = lease;
                let _ = child.wait().await;
            });
        }
    }
}

pub(crate) async fn scan(
    pattern: Pattern<'_>,
    input: &[u8],
    output: &mut Output,
    deadline: Instant,
    cancelled: &mut watch::Receiver<()>,
    lease: Option<Lease>,
) -> Result<(), WorkerError> {
    let executable = std::env::current_exe().map_err(|_| WorkerError::Sandbox)?;
    let child = Command::new(executable)
        .env_clear()
        .current_dir("/")
        .args([
            "--rg-worker",
            if pattern.literal { "literal" } else { "regex" },
            if pattern.sensitive {
                "sensitive"
            } else {
                "insensitive"
            },
            pattern.text,
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .process_group(0)
        .kill_on_drop(true)
        .spawn()
        .map_err(|error| {
            if matches!(error.raw_os_error(), Some(libc::EAGAIN | libc::ENOMEM)) {
                WorkerError::Budget
            } else {
                WorkerError::Sandbox
            }
        })?;
    let mut process = Process {
        child: Some(child),
        lease,
    };
    let child = process.child();
    let mut stdin = child.stdin.take().expect("worker stdin pipe");
    let stdout = child.stdout.take().expect("worker stdout pipe");
    let mut stderr = child
        .stderr
        .take()
        .expect("worker stderr pipe")
        .take(STDERR_BYTES + 1);
    let mut diagnostics = Vec::new();
    let operation = async {
        let write = async move {
            match stdin.write_all(input).await {
                Ok(()) => Ok(()),
                Err(error) if error.kind() == io::ErrorKind::BrokenPipe => Ok(()),
                Err(_) => Err(WorkerError::Unavailable),
            }
        };
        let diagnostic = async {
            stderr
                .read_to_end(&mut diagnostics)
                .await
                .map_err(|_| WorkerError::Unavailable)?;
            if diagnostics.len() as u64 > STDERR_BYTES {
                return Err(WorkerError::Budget);
            }
            Ok(())
        };
        let (summary, (), ()) = tokio::try_join!(output.read(stdout, input), write, diagnostic)?;
        let status = process.reap().await?;
        use std::os::unix::process::ExitStatusExt;
        if matches!(
            status.signal(),
            Some(libc::SIGXCPU | libc::SIGKILL | libc::SIGABRT)
        ) {
            return Ok(Err(WorkerError::Budget));
        }
        Ok(match status.code() {
            Some(125) => Err(WorkerError::Sandbox),
            Some(2)
                if diagnostics.starts_with(b"rg: regex parse error:")
                    || diagnostics.starts_with(b"regex parse error:")
                    || diagnostics.starts_with(b"rg: the literal") =>
            {
                Err(WorkerError::InvalidRegex)
            }
            Some(0 | 1) if summary && diagnostics.is_empty() => Ok(()),
            _ => Err(WorkerError::Unavailable),
        })
    };
    let unfinished = tokio::select! {
        biased;
        _ = cancelled.changed() => Err(WorkerError::Cancelled),
        result = timeout_at(deadline, operation) => match result {
            Ok(Ok(result)) => return result,
            Ok(Err(error)) => Err(error),
            Err(_) => Err(WorkerError::Deadline),
        }
    };
    Process::kill(process.child());
    process.reap().await?;
    unfinished
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::{sync::Semaphore, time::timeout};

    #[tokio::test]
    async fn dropped_worker_releases_its_slot_after_reaping() {
        let permits = Arc::new(Semaphore::new(1));
        let permit = permits.clone().acquire_owned().await.unwrap();
        let child = Command::new("sleep")
            .arg("30")
            .process_group(0)
            .spawn()
            .unwrap();
        let group = -(child.id().unwrap() as libc::pid_t);
        let process = Process {
            child: Some(child),
            lease: Some((File::open("/dev/null").unwrap(), Arc::new(permit))),
        };
        drop(process);
        let _permit = timeout(Duration::from_secs(2), permits.acquire())
            .await
            .unwrap()
            .unwrap();
        // SAFETY: signal 0 checks existence without sending a signal.
        assert_eq!(unsafe { libc::kill(group, 0) }, -1);
        assert_eq!(io::Error::last_os_error().raw_os_error(), Some(libc::ESRCH));
    }

    #[test]
    fn worker_cannot_supply_bytes_outside_the_selected_line() {
        let mut output = Output {
            remaining: 100,
            matches: Vec::new(),
            after: None,
            wanted: 1,
        };
        let false_line = Line {
            absolute_offset: 1,
            lines: Text::Plain {
                text: "has match\n".into(),
            },
            submatches: vec![Submatch {
                content: Text::Plain {
                    text: "match".into(),
                },
                start: 4,
                end: 9,
            }],
        };
        assert_eq!(
            output.line(false_line, b"xhas match\n", 0),
            Err(WorkerError::Unavailable)
        );
        assert!(output.matches.is_empty());
    }
}
