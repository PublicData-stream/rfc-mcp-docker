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
    pub line_number: usize,
    pub end_line_number: usize,
}

#[derive(Clone, Copy)]
pub(crate) struct Pattern<'a> {
    pub text: &'a str,
    pub literal: bool,
    pub sensitive: bool,
    pub multiline: bool,
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
    line_number: usize,
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
        multiline: bool,
    ) -> Result<bool, WorkerError> {
        let mut reader = BufReader::new(stdout);
        let mut frame = Vec::new();
        let mut summary = false;
        let mut previous = None;
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
                    previous = Some(self.line(line, input, previous, multiline)?);
                }
            }
        }
        Err(WorkerError::Unavailable)
    }

    fn line(
        &mut self,
        line: Line,
        input: &[u8],
        previous: Option<(usize, usize, usize)>,
        multiline: bool,
    ) -> Result<(usize, usize, usize), WorkerError> {
        let bytes = line.lines.into_bytes()?;
        let start = line.absolute_offset;
        let end = start
            .checked_add(bytes.len())
            .ok_or(WorkerError::Unavailable)?;
        let (previous_start, previous_end, previous_line) = previous.unwrap_or((0, 0, 1));
        let prefix = input
            .get(previous_start..start)
            .ok_or(WorkerError::Unavailable)?;
        if start < previous_end
            || line.line_number != previous_line + memchr::memchr_iter(b'\n', prefix).count()
            || bytes.is_empty()
            || input.get(start..end) != Some(bytes.as_slice())
            || (start > 0 && input[start - 1] != b'\n')
            || (end < input.len() && bytes.last() != Some(&b'\n'))
            || (!multiline && bytes[..bytes.len() - 1].contains(&b'\n'))
        {
            return Err(WorkerError::Unavailable);
        }
        // The pinned JSON printer omits an empty final submatch at EOF.
        if line.submatches.is_empty() && end == input.len() && bytes.last() != Some(&b'\n') {
            if self.after.is_none_or(|after| (end, end) > after) {
                if self.matches.len() == self.wanted {
                    return Err(WorkerError::Page);
                }
                self.matches.push(Match {
                    start: end,
                    end,
                    line: start..end,
                    line_number: line.line_number,
                    end_line_number: line.line_number,
                });
            }
            return Ok((start, end, line.line_number));
        }
        let mut previous = None;
        let mut newline = memchr::memchr_iter(b'\n', &bytes).peekable();
        let mut start_line = line.line_number;
        let mut start_line_byte = start;
        for submatch in line.submatches {
            let pair = (submatch.start, submatch.end);
            if previous.is_some_and(|prior: (usize, usize)| prior >= pair || prior.1 > pair.0)
                || bytes.get(pair.0..pair.1) != Some(submatch.content.into_bytes()?.as_slice())
            {
                return Err(WorkerError::Unavailable);
            }
            previous = Some(pair);
            let absolute = (start + pair.0, start + pair.1);
            while let Some(&offset) = newline.peek().filter(|&&offset| offset < pair.0) {
                start_line += 1;
                start_line_byte = start + offset + 1;
                newline.next();
            }
            let last = pair.1.saturating_sub(1).max(pair.0);
            let mut end_line = start_line;
            let mut end_line_byte = end;
            let mut last_line_start = start_line_byte;
            while let Some(&offset) = newline.peek() {
                if offset >= last {
                    end_line_byte = start + offset + 1;
                    break;
                }
                newline.next();
                end_line += 1;
                last_line_start = start + offset + 1;
            }
            if self.after.is_none_or(|after| absolute > after) {
                if self.matches.len() == self.wanted {
                    return Err(WorkerError::Page);
                }
                self.matches.push(Match {
                    start: absolute.0,
                    end: absolute.1,
                    line: start_line_byte..end_line_byte,
                    line_number: start_line,
                    end_line_number: end_line,
                });
            }
            start_line = end_line;
            start_line_byte = last_line_start;
        }
        Ok((start, end, line.line_number))
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
            if pattern.multiline {
                "multiline"
            } else {
                "line"
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
        let (summary, (), ()) = tokio::try_join!(
            output.read(stdout, input, pattern.multiline),
            write,
            diagnostic
        )?;
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
    fn multiline_frame_keeps_submatch_line_numbers_and_original_ranges() {
        let input = b"skip\r\na\r\nb z\r\n";
        let line: Line = serde_json::from_value(serde_json::json!({
            "absolute_offset": 6,
            "line_number": 2,
            "lines": {"text": "a\r\nb z\r\n"},
            "submatches": [
                {"match": {"text": "a\r\nb"}, "start": 0, "end": 4},
                {"match": {"text": "z"}, "start": 5, "end": 6}
            ]
        }))
        .unwrap();
        let mut output = Output {
            remaining: 1024,
            matches: Vec::new(),
            after: None,
            wanted: 3,
        };
        output.line(line, input, None, true).unwrap();
        assert_eq!(
            output.matches,
            [
                Match {
                    start: 6,
                    end: 10,
                    line: 6..14,
                    line_number: 2,
                    end_line_number: 3
                },
                Match {
                    start: 11,
                    end: 12,
                    line: 9..14,
                    line_number: 3,
                    end_line_number: 3
                },
            ]
        );
    }

    #[test]
    fn native_empty_eof_submatch_uses_the_verified_end_offset() {
        let line: Line = serde_json::from_value(serde_json::json!({
            "absolute_offset": 0, "line_number": 1,
            "lines": {"text": "abc"}, "submatches": []
        }))
        .unwrap();
        let mut output = Output {
            remaining: 1024,
            matches: Vec::new(),
            after: None,
            wanted: 1,
        };
        output.line(line, b"abc", None, true).unwrap();
        assert_eq!(
            output.matches,
            [Match {
                start: 3,
                end: 3,
                line: 0..3,
                line_number: 1,
                end_line_number: 1,
            }]
        );
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
            line_number: 1,
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
            output.line(false_line, b"xhas match\n", None, false),
            Err(WorkerError::Unavailable)
        );
        assert!(output.matches.is_empty());
    }
}
