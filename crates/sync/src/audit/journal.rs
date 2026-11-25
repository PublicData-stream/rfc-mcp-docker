//! Private crash-recoverable audit state. Every frame is bounded and fsynced.

use super::{BatchReceipt, Input, Repair, State, transfer::open_regular};
use rfc_core::BlobDigest;
use rfc_storage::postgres::Result;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{self, BufReader, Read, Seek, Write},
    os::unix::fs::OpenOptionsExt,
    path::{Path, PathBuf},
};

const JOURNAL_CAP: u64 = 32 * 1024 * 1024; // One file for each of full and sample.
const FRAME_CAP: usize = 512 * 1024;

#[derive(Deserialize, Serialize)]
enum Event {
    Begin {
        selection: String,
        started_us: i64,
    },
    Batch {
        session: BatchReceipt,
        results: Vec<(Input, bool)>,
    },
    Repair {
        input: Input,
        repair: Box<Repair>,
    },
    Complete,
}

pub(super) struct Journal {
    file: File,
    path: PathBuf,
    pub started_us: i64,
    pub states: BTreeMap<u32, State>,
    complete: bool,
}

impl Journal {
    pub fn open(root: &Path, selection: &str, now: i64, lifetime_us: i64) -> Result<Self> {
        let path = root.join("journal");
        if let Some(mut previous) = Self::load(&path, selection, now, lifetime_us)?
            && !previous.complete
        {
            previous.file.seek(io::SeekFrom::End(0))?;
            return Ok(previous);
        }
        let temp = root.join("journal.new");
        super::remove_regular(&temp)?;
        let mut journal = Self {
            file: OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&temp)?,
            path,
            started_us: now,
            states: BTreeMap::new(),
            complete: false,
        };
        journal.append(&Event::Begin {
            selection: selection.into(),
            started_us: now,
        })?;
        fs::rename(temp, &journal.path)?;
        File::open(root)?.sync_all()?;
        Ok(journal)
    }

    fn load(path: &Path, selection: &str, now: i64, lifetime_us: i64) -> Result<Option<Self>> {
        let file = match open_regular(path) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        if file.metadata()?.len() > JOURNAL_CAP {
            return Err("audit journal exceeds its byte cap".into());
        }
        let mut reader = BufReader::new(file);
        let Some(Event::Begin {
            selection: key,
            started_us,
        }) = read_frame(&mut reader)?
        else {
            return Err("audit journal has no cycle header".into());
        };
        if key != selection || now < started_us || now - started_us >= lifetime_us {
            return Ok(None);
        }
        let mut journal = Self {
            file: OpenOptions::new()
                .read(true)
                .write(true)
                .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
                .open(path)?,
            path: path.to_owned(),
            started_us,
            states: BTreeMap::new(),
            complete: false,
        };
        loop {
            let valid_end = reader.stream_position()?;
            match read_frame(&mut reader) {
                Ok(Some(event)) => journal.apply(event)?,
                Ok(None) => break,
                Err(error)
                    if error
                        .downcast_ref::<io::Error>()
                        .is_some_and(|error| error.kind() == io::ErrorKind::UnexpectedEof) =>
                {
                    // A torn last write has no checksum and was never committed.
                    journal.file.set_len(valid_end)?;
                    journal.file.sync_all()?;
                    break;
                }
                Err(error) => return Err(error),
            }
        }
        Ok(Some(journal))
    }

    fn append(&mut self, event: &Event) -> Result<()> {
        let bytes = serde_json::to_vec(event)?;
        if bytes.len() > FRAME_CAP
            || self.file.metadata()?.len() + bytes.len() as u64 + 36 > JOURNAL_CAP
        {
            return Err("audit journal is full".into());
        }
        let hash = BlobDigest::hash(&bytes);
        self.file.write_all(&(bytes.len() as u32).to_be_bytes())?;
        self.file.write_all(&bytes)?;
        self.file.write_all(hash.as_bytes())?;
        self.file.sync_data()?;
        Ok(())
    }

    fn apply(&mut self, event: Event) -> Result<()> {
        if self.complete {
            return Err("audit journal has records after completion".into());
        }
        match event {
            Event::Begin { .. } => return Err("duplicate audit cycle header".into()),
            Event::Batch { results, .. } => {
                for (input, matched) in results {
                    let state = if matched {
                        State::Matched(input)
                    } else {
                        State::Repair(input, Box::default())
                    };
                    self.states.insert(state.rfc(), state);
                }
            }
            Event::Repair { input, repair } => {
                self.states.insert(input.rfc, State::Repair(input, repair));
            }
            Event::Complete => self.complete = true,
        }
        Ok(())
    }

    pub fn batch(&mut self, session: BatchReceipt, results: Vec<(Input, bool)>) -> Result<()> {
        let event = Event::Batch { session, results };
        self.append(&event)?;
        self.apply(event)
    }

    pub fn repair(&mut self, input: Input, repair: Repair) -> Result<()> {
        let event = Event::Repair {
            input,
            repair: Box::new(repair),
        };
        self.append(&event)?;
        self.apply(event)
    }

    pub fn complete(&mut self) -> Result<()> {
        self.append(&Event::Complete)?;
        self.apply(Event::Complete)
    }

    pub fn invalidate(self) -> Result<()> {
        fs::remove_file(&self.path)?;
        File::open(self.path.parent().ok_or("invalid audit journal path")?)?.sync_all()?;
        Ok(())
    }
}

fn read_frame(reader: &mut impl Read) -> Result<Option<Event>> {
    let mut size = [0; 4];
    if reader.read(&mut size[..1])? == 0 {
        return Ok(None);
    }
    reader.read_exact(&mut size[1..])?;
    let length = u32::from_be_bytes(size) as usize;
    if !(1..=FRAME_CAP).contains(&length) {
        return Err("audit journal frame exceeds its bounds".into());
    }
    let mut bytes = vec![0; length];
    reader.read_exact(&mut bytes)?;
    let mut hash = [0; 32];
    reader.read_exact(&mut hash)?;
    if BlobDigest::hash(&bytes).as_bytes() != &hash {
        return Err("audit journal frame checksum failed".into());
    }
    Ok(Some(serde_json::from_slice(&bytes)?))
}
