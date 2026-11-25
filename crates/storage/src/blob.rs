//! Linux filesystem operations for the Compose mirror volume.

use rfc_core::{BlobDigest, ByteRange, GenerationId};
use rustix::fs::{
    self, AtFlags, CWD, Dir, FileType, FlockOperation, Mode, OFlags, RenameFlags, ResolveFlags,
    StatVfsMountFlags,
};
use rustix::io::Errno;
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::os::fd::{AsRawFd, OwnedFd};
use std::path::Path;

const RESOLVE: ResolveFlags = ResolveFlags::BENEATH
    .union(ResolveFlags::NO_SYMLINKS)
    .union(ResolveFlags::NO_XDEV);
const DIRECTORY: OFlags = OFlags::RDONLY
    .union(OFlags::DIRECTORY)
    .union(OFlags::CLOEXEC);
const PUBLIC_DIRECTORY: Mode = Mode::from_raw_mode(0o755);
const PRIVATE_DIRECTORY: Mode = Mode::from_raw_mode(0o700);

/// The canonical relative path for a digest.
pub fn relative_path(digest: BlobDigest) -> String {
    let hex = digest.to_string();
    format!("blobs/blake3/{}/{hex}", &hex[..2])
}

fn mirror(path: &Path) -> io::Result<OwnedFd> {
    if !path.is_absolute() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "absolute mirror root required",
        ));
    }
    Ok(fs::openat2(
        CWD,
        path,
        DIRECTORY,
        Mode::empty(),
        ResolveFlags::NO_SYMLINKS,
    )?)
}

fn read_only(root: &OwnedFd) -> io::Result<bool> {
    Ok(fs::fstatvfs(root)?
        .f_flag
        .contains(StatVfsMountFlags::RDONLY))
}

fn directory(parent: &OwnedFd, name: &str) -> io::Result<OwnedFd> {
    Ok(fs::openat2(
        parent,
        name,
        DIRECTORY,
        Mode::empty(),
        RESOLVE,
    )?)
}

fn ensure_directory(parent: &OwnedFd, name: &str, mode: Mode) -> io::Result<OwnedFd> {
    match fs::mkdirat(parent, name, mode) {
        Ok(()) | Err(Errno::EXIST) => {}
        Err(error) => return Err(error.into()),
    }
    let child = directory(parent, name)?;
    // A concurrent installer may have created the entry and exited before its sync.
    fs::fsync(parent)?;
    Ok(child)
}

fn regular(root: &OwnedFd, path: &str) -> io::Result<File> {
    let handle = fs::openat2(
        root,
        path,
        OFlags::PATH | OFlags::CLOEXEC,
        Mode::empty(),
        RESOLVE,
    )?;
    let stat = fs::fstat(&handle)?;
    if FileType::from_raw_mode(stat.st_mode) != FileType::RegularFile || stat.st_nlink != 1 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "expected a singly linked regular file",
        ));
    }
    // Reopen the checked inode through procfs while keeping the O_PATH handle.
    // This avoids opening a device or blocking on a FIFO after a pathname swap.
    let path = format!("/proc/self/fd/{}", handle.as_raw_fd());
    Ok(File::from(fs::open(
        path.as_str(),
        OFlags::RDONLY | OFlags::CLOEXEC,
        Mode::empty(),
    )?))
}

/// A read-only view of original blobs.
pub struct BlobReader {
    root: OwnedFd,
}

impl BlobReader {
    /// Opens a mirror. Serving callers must require a read-only mount.
    pub fn open(path: &Path, require_read_only: bool) -> io::Result<Self> {
        let root = mirror(path)?;
        if require_read_only && !read_only(&root)? {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "serve requires a read-only mirror mount",
            ));
        }
        directory(&root, "blobs/blake3")?;
        Ok(Self { root })
    }

    /// Opens the canonical original bytes for bounded streaming.
    pub fn open_blob(&self, digest: BlobDigest) -> io::Result<File> {
        regular(&self.root, &relative_path(digest))
    }

    /// Rehashes a canonical original without modifying or syncing the mirror.
    pub fn verify_blob(&self, digest: BlobDigest, byte_length: i64) -> io::Result<()> {
        verify_contents(
            self.open_blob(digest)?,
            DurableBlob {
                digest,
                byte_length,
            },
        )?;
        Ok(())
    }

    pub(crate) fn generation_fence(&self, generation: GenerationId) -> io::Result<File> {
        generation_fence(
            &self.root,
            generation,
            FlockOperation::NonBlockingLockShared,
        )
    }

    /// Reads a range with an explicit allocation ceiling.
    pub fn read_range(
        &self,
        digest: BlobDigest,
        range: ByteRange,
        limit: usize,
    ) -> io::Result<Vec<u8>> {
        let mut file = self.open_blob(digest)?;
        let length = usize::try_from(range.end() - range.start())
            .ok()
            .filter(|length| *length <= limit && *length <= isize::MAX as usize);
        let blob_length = file.metadata()?.len();
        let length = length.filter(|_| range.fits(blob_length));
        let length = length.ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "blob range exceeds bounds")
        })?;
        file.seek(SeekFrom::Start(range.start()))?;
        let mut bytes = Vec::new();
        bytes.try_reserve_exact(length).map_err(io::Error::other)?;
        bytes.resize(length, 0);
        file.read_exact(&mut bytes)?;
        Ok(bytes)
    }
}

fn generation_fence(
    root: &OwnedFd,
    generation: GenerationId,
    operation: FlockOperation,
) -> io::Result<File> {
    let file = regular(root, &format!("generation-locks/{}", generation.get()))?;
    fs::flock(&file, operation)?;
    Ok(file)
}

/// A blob verified and synchronized before database registration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DurableBlob {
    digest: BlobDigest,
    byte_length: i64,
}

impl DurableBlob {
    pub fn digest(self) -> BlobDigest {
        self.digest
    }

    pub fn byte_length(self) -> i64 {
        self.byte_length
    }
}

/// Result of an original-byte streaming installation.
#[derive(Debug)]
pub enum Install {
    New(DurableBlob),
    Reused(DurableBlob),
}

impl Install {
    pub fn blob(&self) -> DurableBlob {
        match self {
            Self::New(blob) | Self::Reused(blob) => *blob,
        }
    }
}

/// Writer used by the synchronization role.
pub struct BlobWriter {
    root: OwnedFd,
    blobs: OwnedFd,
    staging: OwnedFd,
    quarantine: OwnedFd,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Boundary {
    BeforeFileSync,
    AfterFileSync,
    AfterRename,
    AfterDirectorySync,
}

impl BlobWriter {
    /// The mirror inode remains locked across database failures and is never replaced.
    pub fn coordinate(path: &Path) -> io::Result<OwnedFd> {
        let root = mirror(path)?;
        if read_only(&root)? {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "sync requires a writable mirror mount",
            ));
        }
        fs::flock(&root, FlockOperation::NonBlockingLockExclusive)?;
        Ok(root)
    }

    fn writable_root(path: &Path) -> io::Result<OwnedFd> {
        let root = mirror(path)?;
        if read_only(&root)? {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "sync requires a writable mirror mount",
            ));
        }
        fs::accessat(
            &root,
            ".",
            fs::Access::WRITE_OK | fs::Access::EXEC_OK,
            AtFlags::EACCESS,
        )?;
        Ok(root)
    }

    /// Inspects the Sync mirror mount without preparing or changing directories.
    pub fn check_mount(path: &Path) -> io::Result<()> {
        Self::writable_root(path).map(drop)
    }

    /// Opens a writable mirror and durably prepares its private directories.
    pub fn open(path: &Path) -> io::Result<Self> {
        let root = Self::writable_root(path)?;
        let blobs = ensure_directory(&root, "blobs", PUBLIC_DIRECTORY)?;
        let blobs = ensure_directory(&blobs, "blake3", PUBLIC_DIRECTORY)?;
        let staging = ensure_directory(&root, "staging", PRIVATE_DIRECTORY)?;
        let quarantine = ensure_directory(&root, "quarantine", PRIVATE_DIRECTORY)?;
        for private in [&staging, &quarantine] {
            if fs::fstat(private)?.st_mode & 0o077 != 0 {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "staging and quarantine must be private",
                ));
            }
        }
        for writable in [&blobs, &staging, &quarantine] {
            fs::accessat(
                writable,
                ".",
                fs::Access::WRITE_OK | fs::Access::EXEC_OK,
                AtFlags::EACCESS,
            )?;
        }
        Ok(Self {
            root,
            blobs,
            staging,
            quarantine,
        })
    }

    pub fn prepare_generation_fence(&self, generation: GenerationId) -> io::Result<()> {
        let locks = ensure_directory(&self.root, "generation-locks", PUBLIC_DIRECTORY)?;
        let name = generation.get().to_string();
        match fs::openat2(
            &locks,
            name.as_str(),
            OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::CLOEXEC,
            Mode::from_raw_mode(0o444),
            RESOLVE,
        ) {
            Ok(file) => fs::fsync(file)?,
            Err(Errno::EXIST) => regular(&locks, &name)?.sync_all()?,
            Err(error) => return Err(error.into()),
        }
        fs::fsync(&locks)?;
        Ok(())
    }

    pub fn try_retirement_fence(&self, generation: GenerationId) -> io::Result<Option<File>> {
        match generation_fence(
            &self.root,
            generation,
            FlockOperation::NonBlockingLockExclusive,
        ) {
            Ok(file) => Ok(Some(file)),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => Ok(None),
            Err(error) => Err(error),
        }
    }

    pub fn verify_generation_fence(&self, generation: GenerationId) -> io::Result<()> {
        regular(
            &self.root,
            &format!("generation-locks/{}", generation.get()),
        )?
        .sync_all()
    }

    /// Verify canonical bytes and flush their existing name before publication.
    pub fn verify_durable(&self, digest: BlobDigest, byte_length: i64) -> io::Result<DurableBlob> {
        let blob = DurableBlob {
            digest,
            byte_length,
        };
        self.verify_existing(blob)?;
        let hex = digest.to_string();
        fs::fsync(directory(&self.blobs, &hex[..2])?)?;
        Ok(blob)
    }

    /// Inventory a canonical shard without following an entry supplied by the filesystem.
    pub fn inventory_shard(&self, index: u8) -> io::Result<Vec<BlobDigest>> {
        let name = format!("{index:02x}");
        let shard = match directory(&self.blobs, &name) {
            Ok(shard) => shard,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(error),
        };
        let mut digests = Vec::new();
        for entry in Dir::read_from(&shard)? {
            let entry = entry?;
            let filename = entry.file_name().to_str().unwrap_or("");
            if matches!(filename, "." | "..") {
                continue;
            }
            if let Some(digest) =
                BlobDigest::from_hex(filename).filter(|_| filename.starts_with(&name))
            {
                digests.push(digest);
            } else {
                eprintln!("preserving an unexpected entry in blob shard {name}");
            }
        }
        Ok(digests)
    }

    /// Remove a queued canonical blob only if its bytes and inode are as expected.
    pub fn remove_queued_blob(&self, digest: BlobDigest, length: Option<i64>) -> io::Result<()> {
        let name = digest.to_string();
        let shard = match directory(&self.blobs, &name[..2]) {
            Ok(shard) => shard,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                fs::fsync(&self.blobs)?;
                return Ok(());
            }
            Err(error) => return Err(error),
        };
        match regular(&shard, &name) {
            Ok(file) => {
                let size = i64::try_from(file.metadata()?.len()).map_err(io::Error::other)?;
                self.verify_existing(DurableBlob {
                    digest,
                    byte_length: length.unwrap_or(size),
                })?;
                crate::fixture_boundary("before_blob_unlink");
                fs::unlinkat(&shard, name.as_str(), AtFlags::empty())?;
                crate::fixture_boundary("after_blob_unlink");
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        fs::fsync(&shard)?;
        crate::fixture_boundary("after_unlink_sync");
        Ok(())
    }

    /// Reconcile only known install-stage layouts while holding the coordinator.
    pub fn recover_staging(&self) -> io::Result<usize> {
        let mut removed = 0;
        for entry in Dir::read_from(&self.staging)? {
            let entry = entry?;
            let name = entry.file_name().to_str().unwrap_or("");
            if matches!(name, "." | "..") {
                continue;
            }
            if name.len() != 32
                || !name
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            {
                eprintln!("preserving an unexpected staging entry");
                continue;
            }
            let stage = directory(&self.staging, name)?;
            let mut body = false;
            let mut evidence = false;
            for file in Dir::read_from(&stage)? {
                let file = file?;
                match file.file_name().to_str().unwrap_or("") {
                    "." | ".." => {}
                    "body" if regular(&stage, "body").is_ok() => body = true,
                    _ => evidence = true,
                }
            }
            if evidence {
                self.move_to_quarantine(name, &stage)?;
                continue;
            }
            if body {
                fs::unlinkat(&stage, "body", AtFlags::empty())?;
            }
            fs::unlinkat(&self.staging, name, AtFlags::REMOVEDIR)?;
            fs::fsync(&self.staging)?;
            removed += 1;
        }
        Ok(removed)
    }

    /// Opens a regular file beneath an explicitly selected trusted source root.
    pub fn open_candidate(&self, source_root: &Path, relative: &str) -> io::Result<File> {
        if relative
            .split('/')
            .any(|part| matches!(part, "" | "." | ".."))
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid candidate path",
            ));
        }
        regular(&mirror(source_root)?, relative)
    }

    /// Streams bytes with an explicit maximum representable PostgreSQL length.
    pub fn install(&self, input: impl Read, max_bytes: u64) -> io::Result<Install> {
        self.install_with(input, max_bytes, |boundary| {
            crate::fixture_boundary(match boundary {
                Boundary::BeforeFileSync => "before_file_sync",
                Boundary::AfterFileSync => "after_file_sync",
                Boundary::AfterRename => "after_blob_install",
                Boundary::AfterDirectorySync => "after_directory_sync",
            });
        })
    }

    /// Keeps mismatching source captures out of the canonical blob namespace.
    pub fn quarantine_mismatch(
        &self,
        rsync: &[u8],
        https: &[u8],
        max_bytes: usize,
    ) -> io::Result<()> {
        if rsync.len() > max_bytes || https.len() > max_bytes || rsync == https {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid mismatch evidence",
            ));
        }
        let (name, stage) = new_stage(&self.staging)?;
        for (filename, bytes) in [("body", rsync), ("https-body", https)] {
            let mut file = create(&stage, filename)?;
            file.write_all(bytes)?;
            file.sync_all()?;
        }
        let mut incident = create(&stage, "incident.txt")?;
        writeln!(
            incident,
            "rsync={} rsync_length={} https={} https_length={}",
            BlobDigest::hash(rsync),
            rsync.len(),
            BlobDigest::hash(https),
            https.len()
        )?;
        incident.sync_all()?;
        self.move_to_quarantine(&name, &stage)
    }

    fn install_with(
        &self,
        input: impl Read,
        max_bytes: u64,
        hook: impl Fn(Boundary),
    ) -> io::Result<Install> {
        let cap = i64::try_from(max_bytes).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "blob limit exceeds bigint")
        })?;
        let (name, stage) = new_stage(&self.staging)?;
        let result = self.install_staged(&name, &stage, input, cap, hook);
        // A quarantined directory has moved. All other unsuccessful or reused
        // stages can be cleaned using the private directory descriptor.
        match fs::unlinkat(&self.staging, name.as_str(), AtFlags::REMOVEDIR) {
            Ok(()) => fs::fsync(&self.staging)?,
            Err(Errno::NOENT) => {}
            Err(Errno::NOTEMPTY) if result.is_err() => {
                if fs::statat(&stage, "incident.txt", AtFlags::SYMLINK_NOFOLLOW).is_ok() {
                    return result;
                }
                fs::unlinkat(&stage, "body", AtFlags::empty())?;
                fs::unlinkat(&self.staging, name.as_str(), AtFlags::REMOVEDIR)?;
                fs::fsync(&self.staging)?;
            }
            Err(error) => return Err(error.into()),
        }
        result
    }

    fn install_staged(
        &self,
        name: &str,
        stage: &OwnedFd,
        input: impl Read,
        cap: i64,
        hook: impl Fn(Boundary),
    ) -> io::Result<Install> {
        let mut file = create(stage, "body")?;
        let mut reader = input.take(cap as u64 + 1);
        let (digest, byte_length) = hash_stream(&mut reader, &mut file, cap)?;
        let blob = DurableBlob {
            digest,
            byte_length,
        };
        fs::fchmod(&file, Mode::from_raw_mode(0o444))?;
        hook(Boundary::BeforeFileSync);
        file.sync_all()?;
        drop(file);
        hook(Boundary::AfterFileSync);

        let hex = digest.to_string();
        let shard = ensure_directory(&self.blobs, &hex[..2], PUBLIC_DIRECTORY)?;
        match fs::renameat_with(stage, "body", &shard, hex.as_str(), RenameFlags::NOREPLACE) {
            Ok(()) => {
                hook(Boundary::AfterRename);
                fs::fsync(&shard)?;
                fs::fsync(stage)?;
                hook(Boundary::AfterDirectorySync);
                Ok(Install::New(blob))
            }
            Err(Errno::EXIST) => {
                if let Err(error) = self.verify_existing(blob) {
                    self.quarantine(name, stage, blob, &error)?;
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "canonical blob failed verification; candidate quarantined",
                    ));
                }
                // Another process may have installed the inode but crashed
                // before syncing the destination directory.
                fs::fsync(&shard)?;
                fs::unlinkat(stage, "body", AtFlags::empty())?;
                fs::fsync(stage)?;
                Ok(Install::Reused(blob))
            }
            Err(error) => Err(error.into()),
        }
    }

    fn verify_existing(&self, blob: DurableBlob) -> io::Result<()> {
        let file = regular(&self.root, &relative_path(blob.digest))?;
        verify_contents(file, blob)?.sync_all()
    }

    fn quarantine(
        &self,
        name: &str,
        stage: &OwnedFd,
        blob: DurableBlob,
        error: &io::Error,
    ) -> io::Result<()> {
        let mut incident = create(stage, "incident.txt")?;
        writeln!(
            incident,
            "digest={} expected_length={} verification={}",
            blob.digest,
            blob.byte_length,
            error.kind()
        )?;
        incident.sync_all()?;
        self.move_to_quarantine(name, stage)
    }

    fn move_to_quarantine(&self, name: &str, stage: &OwnedFd) -> io::Result<()> {
        fs::fsync(stage)?;
        fs::renameat_with(
            &self.staging,
            name,
            &self.quarantine,
            name,
            RenameFlags::NOREPLACE,
        )?;
        fs::fsync(&self.quarantine)?;
        fs::fsync(&self.staging)?;
        Ok(())
    }
}

fn verify_contents(file: File, blob: DurableBlob) -> io::Result<File> {
    let meta = file.metadata()?;
    use std::os::unix::fs::MetadataExt;
    if meta.len() != blob.byte_length as u64 || meta.mode() & 0o222 != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unexpected canonical blob size or mode",
        ));
    }
    let mut reader = (&file).take(blob.byte_length as u64 + 1);
    let (digest, length) = hash_stream(&mut reader, &mut io::sink(), blob.byte_length)?;
    if (digest, length) != (blob.digest, blob.byte_length) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "canonical blob digest mismatch",
        ));
    }
    Ok(file)
}

fn hash_stream(
    reader: &mut impl Read,
    writer: &mut impl Write,
    cap: i64,
) -> io::Result<(BlobDigest, i64)> {
    let mut hash = blake3::Hasher::new();
    let mut size = 0i64;
    let mut buffer = [0u8; 32 * 1024];
    loop {
        let count = match reader.read(&mut buffer) {
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            result => result?,
        };
        if count == 0 {
            return Ok((BlobDigest::from_bytes(*hash.finalize().as_bytes()), size));
        }
        size = size
            .checked_add(count as i64)
            .filter(|size| *size <= cap)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "blob exceeds byte limit"))?;
        writer.write_all(&buffer[..count])?;
        hash.update(&buffer[..count]);
    }
}

fn create(parent: &OwnedFd, name: &str) -> io::Result<File> {
    Ok(File::from(fs::openat2(
        parent,
        name,
        OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::CLOEXEC,
        Mode::from_raw_mode(0o600),
        RESOLVE,
    )?))
}

fn new_stage(parent: &OwnedFd) -> io::Result<(String, OwnedFd)> {
    for _ in 0..128 {
        let mut random = [0u8; 16];
        getrandom::fill(&mut random).map_err(|error| io::Error::other(error.to_string()))?;
        let name = format!("{:032x}", u128::from_be_bytes(random));
        match fs::mkdirat(parent, name.as_str(), PRIVATE_DIRECTORY) {
            Err(Errno::EXIST) => continue,
            result => result?,
        }
        fs::fsync(parent)?;
        let fd = directory(parent, &name)?;
        return Ok((name, fd));
    }
    Err(io::Error::new(
        io::ErrorKind::AlreadyExists,
        "no unique staging directory",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::{PermissionsExt, symlink};
    use std::process::Command;
    use tempfile::{TempDir, tempdir_in};

    fn work() -> TempDir {
        tempdir_in("/tmp").unwrap()
    }

    #[test]
    fn exact_streamed_bytes_ranges_and_limits() {
        let root = work();
        let writer = BlobWriter::open(root.path()).unwrap();
        let bytes = b"line\r\n\x0c\xc3\xa9\xff\n";
        let first = writer
            .install(bytes.as_slice(), bytes.len() as u64)
            .unwrap();
        assert!(matches!(first, Install::New(_)));
        let digest = first.blob().digest();
        assert_eq!(digest, BlobDigest::hash(bytes));
        assert_eq!(BlobDigest::from_hex(&digest.to_string()), Some(digest));
        assert!(BlobDigest::from_hex(&digest.to_string().to_uppercase()).is_none());
        assert_ne!(
            writer
                .install(&b"Line\r\n\x0c\xc3\xa9\xff\n"[..], 100)
                .unwrap()
                .blob()
                .digest(),
            digest
        );
        assert!(matches!(
            writer.install(bytes.as_slice(), 100).unwrap(),
            Install::Reused(_)
        ));
        assert_eq!(
            writer.install(bytes.as_slice(), 3).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
        assert_eq!(
            std::fs::read_dir(root.path().join("staging"))
                .unwrap()
                .count(),
            0
        );
        let reader = BlobReader::open(root.path(), false).unwrap();
        let range = ByteRange::new(2, 10).unwrap();
        assert_eq!(reader.read_range(digest, range, 9).unwrap(), bytes[2..10]);
        assert!(reader.read_range(digest, range, 7).is_err());
        assert!(
            reader
                .read_range(digest, ByteRange::new(5, 99).unwrap(), 100)
                .is_err()
        );
    }

    #[test]
    fn rejects_unsafe_paths_and_preserves_a_corrupt_canonical_inode() {
        let root = work();
        let writer = BlobWriter::open(root.path()).unwrap();
        let bytes = b"same size";
        let digest = writer
            .install(bytes.as_slice(), 32)
            .unwrap()
            .blob()
            .digest();
        let path = root.path().join(relative_path(digest));
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        std::fs::write(&path, b"bad bytes").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o444)).unwrap();
        assert!(writer.install(bytes.as_slice(), 32).is_err());
        assert_eq!(std::fs::read(&path).unwrap(), b"bad bytes");
        let quarantine = std::fs::read_dir(root.path().join("quarantine"))
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        assert_eq!(std::fs::read(quarantine.join("body")).unwrap(), bytes);
        assert!(
            std::fs::read_to_string(quarantine.join("incident.txt"))
                .unwrap()
                .contains(&digest.to_string())
        );
        let source = work();
        std::fs::hard_link(&path, source.path().join("link")).unwrap();
        symlink(source.path(), root.path().join("outside")).unwrap();
        for (directory, name) in [
            (root.path(), "../anything"),
            (root.path(), "outside/link"),
            (source.path(), "link"),
            (Path::new("/dev"), "null"),
        ] {
            assert!(writer.open_candidate(directory, name).is_err());
        }
    }

    #[test]
    fn process_interruption_at_each_durability_boundary_can_be_retried() {
        for boundary in [
            Boundary::BeforeFileSync,
            Boundary::AfterFileSync,
            Boundary::AfterRename,
            Boundary::AfterDirectorySync,
        ] {
            let root = work();
            let output = Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "blob::tests::crash_helper", "--nocapture"])
                .env("RFC_TEST_CRASH_MIRROR", root.path())
                .env("RFC_TEST_CRASH_BOUNDARY", format!("{boundary:?}"))
                .output()
                .unwrap();
            assert_eq!(output.status.code(), Some(86), "{output:?}");
            let expected = BlobDigest::hash(b"crash fixture");
            let path = root.path().join(relative_path(expected));
            if matches!(
                boundary,
                Boundary::AfterRename | Boundary::AfterDirectorySync
            ) {
                assert_eq!(std::fs::read(&path).unwrap(), b"crash fixture");
            } else {
                assert!(!path.exists());
            }
            let store = BlobWriter::open(root.path()).unwrap();
            let retry = store.install(&b"crash fixture"[..], 64).unwrap();
            assert_eq!(retry.blob().digest(), expected);
            assert_eq!(std::fs::read(&path).unwrap(), b"crash fixture");
        }
    }

    #[test]
    fn crash_helper() {
        let Some(root) = std::env::var_os("RFC_TEST_CRASH_MIRROR") else {
            return;
        };
        let boundary = std::env::var("RFC_TEST_CRASH_BOUNDARY").unwrap();
        BlobWriter::open(Path::new(&root))
            .unwrap()
            .install_with(&b"crash fixture"[..], 64, |step| {
                if format!("{step:?}") == boundary {
                    // Terminate before any Rust destructors or staging cleanup run.
                    std::process::exit(86);
                }
            })
            .unwrap();
        panic!("requested durability boundary was not reached");
    }
}
