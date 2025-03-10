use std::any::Any;
use std::fmt;
use std::fs::Metadata;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::path::PathBuf;

use filetime::FileTime;
use log::{debug, error, info, warn};
// use std::fs::File;
use std::io::Error;
use std::io::{/*BufReader, BufWriter, Read, */Write};
// use std::time::Duration;

use rayon::prelude::*;
// use memmap2::{Mmap, MmapMut};

// use crate::Throttle;

// const BUFFER_SIZE: usize = 128 * 1024;
// const CHUNK_SIZE: usize = BUFFER_SIZE * 1024;

#[derive(Debug)]
pub struct DirError {
    pub path: PathBuf,
    pub error: Error,
}

impl fmt::Display for DirError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Failed to process directory {}: {}",
            self.path.display(),
            self.error
        )
    }
}

#[derive(Debug)]
pub struct SymLinkError {
    pub path: PathBuf,
    pub error: Error,
}

impl fmt::Display for SymLinkError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Failed to process symlink {}: {}",
            self.path.display(),
            self.error
        )
    }
}

#[derive(Debug)]
pub struct FileError {
    pub path: PathBuf,
    pub error: Error,
}

impl fmt::Display for FileError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Failed to process file {}: {}",
            self.path.display(),
            self.error
        )
    }
}

impl std::error::Error for DirError {}
impl std::error::Error for SymLinkError {}
impl std::error::Error for FileError {}

#[derive(Clone, Default, Debug)]
pub struct SyncStatus {
    pub dirs_copied: u64,
    pub dirs_total: u64,
    pub dirs_errors: u64,
    pub files_copied: u64,
    pub files_total: u64,
    pub files_errors: u64,
    pub links_copied: u64,
    pub links_total: u64,
    pub links_errors: u64,
    pub permissions_errors: u64,
    pub bytes_copied: u64,
    pub bytes_total: u64,
}

impl SyncStatus {
    pub fn entries_total(&self) -> u64 {
        self.dirs_total + self.files_total + self.links_total
    }

    pub fn errors_total(&self) -> u64 {
        self.dirs_errors + self.files_errors + self.links_errors + self.permissions_errors
    }

    pub fn copied_total(&self) -> u64 {
        self.dirs_copied + self.files_copied + self.links_copied
    }

    pub fn skipped_total(&self) -> u64 {
        self.entries_total() - self.copied_total()
    }

    pub fn dirs_skipped(&self) -> u64 {
        self.dirs_total - self.dirs_copied
    }

    pub fn files_skipped(&self) -> u64 {
        self.files_total - self.files_copied
    }

    pub fn links_skipped(&self) -> u64 {
        self.links_total - self.links_copied
    }

    pub fn reduce(&self, other: &Self) -> Self {
        SyncStatus {
            dirs_copied: self.dirs_copied + other.dirs_copied,
            dirs_total: self.dirs_total + other.dirs_total,
            dirs_errors: self.dirs_errors + other.dirs_errors,
            files_copied: self.files_copied + other.files_copied,
            files_total: self.files_total + other.files_total,
            files_errors: self.files_errors + other.files_errors,
            links_copied: self.links_copied + other.links_copied,
            links_total: self.links_total + other.links_total,
            links_errors: self.links_errors + other.links_errors,
            permissions_errors: self.permissions_errors + other.permissions_errors,
            bytes_copied: self.bytes_copied + other.bytes_copied,
            bytes_total: self.bytes_total + other.bytes_total,
        }
    }

    pub fn print(self) {
        println!(
            "Items total: {}, copied: {}, skipped: {}, errors: {}, permission errors: {}",
            self.entries_total(),
            self.copied_total(),
            self.skipped_total(),
            self.errors_total(),
            self.permissions_errors
        );
        println!(
            "Directories total: {}, copied: {}, skipped: {}, errors: {}",
            self.dirs_total,
            self.dirs_copied,
            self.dirs_skipped(),
            self.dirs_errors
        );
        println!(
            "Symbolic links total: {}, copied: {}, skipped: {}, errors: {}",
            self.links_total,
            self.links_copied,
            self.links_skipped(),
            self.links_errors
        );
        println!(
            "Files total: {}, copied: {}, skipped: {}, errors: {}",
            self.files_total,
            self.files_copied,
            self.files_skipped(),
            self.files_errors
        );
        println!(
            "Transfered {} bytes out of {}",
            humansize::format_size(self.bytes_copied, humansize::BINARY),
            humansize::format_size(self.bytes_total, humansize::BINARY),
        );
    }
}

#[derive(Debug, Default)]
pub enum SyncResult {
    #[default]
    DirSkipped,
    DirCopied,
    SymLinkSkipped,
    SymLinkCopied,
    FileSkipped(u64),
    FileCopied(u64),
}

#[derive(Copy, Clone)]
pub enum SyncMethod {
    Serial,
    Parallel,
    Mmap,
}

impl SyncMethod {
    pub fn from_str(s: &str) -> Self {
        match s {
            "serial" => Self::Serial,
            "parallel" => Self::Parallel,
            "mmap" => Self::Mmap,
            _ => Self::Serial,
        }
    }

    // pub fn copy_function(self) -> fn(&PathBuf, &PathBuf, &SyncOptions, &Metadata) -> Result<u64, Error> {
    //     match self {
    //         Self::Serial => copy_file_serial,
    //         Self::Parallel => copy_file_parallel,
    //         Self::Mmap => copy_file_memmap,
    //     }
    // }
}

#[derive(Copy, Clone)]
pub struct SyncOptions {
    /// Wether to preserve permissions of the source file after the destination is written.
    pub preserve_permissions: bool,
    pub perform_dry_run: bool,
    pub parallelism: usize,
    pub sync_method: SyncMethod,
}

impl Default for SyncOptions {
    fn default() -> Self {
        Self {
            preserve_permissions: true,
            perform_dry_run: false,
            parallelism: 1,
            sync_method: SyncMethod::Serial,
        }
    }
}

pub fn sync(
    mut _out: impl Write,
    mut _err: Option<impl Write>,
    source_path: &Path,
    destination_path: &Path,
    options: &SyncOptions,
) -> SyncStatus {
    if !source_path.is_dir() {
        error!("Source path is not a directory: {}", source_path.display());
        let mut status = SyncStatus::default();
        status.dirs_errors = 1;
        return status;
    }
    if !destination_path.is_dir() && !options.perform_dry_run {
        if let Err(e) = std::fs::create_dir_all(&destination_path) {
            error!(
                "Failed to create directory: {}, {}",
                destination_path.display(),
                e
            );
            let mut status = SyncStatus::default();
            status.dirs_errors = 1;
            return status;
        }
    }
    sync_path(source_path, destination_path, options)
}

fn sync_path(source_base: &Path, destination_base: &Path, options: &SyncOptions) -> SyncStatus {
    let source_dir = std::fs::read_dir(source_base);
    if source_dir.is_err() {
        let mut status = SyncStatus::default();
        status.permissions_errors = 1;
        return status;
    }
    source_dir
        .unwrap()
        .par_bridge()
        .map(|entry| {
            if entry.is_err() {
                let mut status = SyncStatus::default();
                status.permissions_errors = 1;
                return status;
            }
            let entry = entry.unwrap();
            let source_path = entry.path();
            let metadata = entry.metadata();
            if metadata.is_err() {
                let mut status = SyncStatus::default();
                status.dirs_errors = 1;
                return status;
            }
            let metadata = metadata.unwrap();
            let source_relative = source_path.strip_prefix(&*source_base).unwrap();
            let destination_path = destination_base.join(&source_relative);
            if metadata.is_dir() {
                debug!(
                    "Syncing directory: {} -> {}",
                    source_path.display(),
                    destination_path.display()
                );
                let status = sync_dir(&source_path, &destination_path, &options, &metadata);
                status.reduce(&sync_path(&source_path, &destination_path, options))
            } else if metadata.is_symlink() {
                debug!(
                    "Syncing symlink: {} -> {}",
                    source_path.display(),
                    destination_path.display()
                );
                sync_symlink(&source_path, &destination_path, &options, &metadata)
            } else {
                debug!(
                    "Syncing file: {} -> {}",
                    source_path.display(),
                    destination_path.display()
                );
                sync_file(&source_path, &destination_path, &options, &metadata)
            }
        })
        .reduce(|| SyncStatus::default(), |a, b| a.reduce(&b))
}

fn copy_permissions(metadata: &Metadata, destination: &PathBuf) -> Result<(), Error> {
    let permissions = metadata.permissions();
    debug!(
        "Setting permissions {:o} on {}",
        permissions.mode(),
        destination.display()
    );
    std::fs::set_permissions(&destination, permissions)
}

fn sync_dir(
    _source: &PathBuf,
    destination: &PathBuf,
    options: &SyncOptions,
    metadata: &Metadata,
) -> SyncStatus {
    let mut status = SyncStatus::default();
    status.dirs_total = 1;
    if !destination.exists() {
        if options.perform_dry_run {
            debug!("Would create directory: {}", destination.display());
        } else {
            // assert_parent_exists(&destination)?;
            debug!("Creating directory: {}", destination.display());
            if let Err(e) = std::fs::create_dir_all(&destination) {
                error!(
                    "Failed to create directory: {}, {}",
                    destination.display(),
                    e
                );
                status.dirs_errors = 1;
                return status;
            }
        }
        info!("Created directory: {}", destination.display());
        status.dirs_copied = 1;
    }
    #[cfg(unix)]
    {
        if !options.perform_dry_run && options.preserve_permissions {
            if let Err(e) = copy_permissions(&metadata, destination) {
                error!(
                    "Failed to set permissions on directory: {}, {}",
                    destination.display(),
                    e
                );
                status.permissions_errors = 1;
            }
        }
    }
    status
}

fn sync_symlink(
    source: &PathBuf,
    destination: &PathBuf,
    options: &SyncOptions,
    _metadata: &Metadata,
) -> SyncStatus {
    let mut status = SyncStatus::default();
    status.links_total = 1;
    let link = std::fs::read_link(&source);
    if link.is_err() {
        error!(
            "Failed to read symlink: {}, {}",
            source.display(),
            link.unwrap_err()
        );
        status.links_errors = 1;
        return status;
    }
    let link = link.unwrap();
    let destination_metadata = destination.symlink_metadata();
    if destination_metadata.is_ok() {
        let destination_metadata = destination_metadata.unwrap();
        if destination_metadata.file_type().is_symlink() {
            let destination_link = std::fs::read_link(&destination).unwrap();
            if destination_link == link {
                return status;
            }
        }
        if options.perform_dry_run {
            debug!("Would delete existing file: {}", destination.display());
        } else {
            debug!("Deleting existing file: {}", destination.display());
            if let Err(e) = std::fs::remove_file(&destination) {
                status.links_errors = 1;
                error!(
                    "Failed to delete existing file: {}, {}",
                    destination.display(),
                    e
                );
                return status;
            }
        }
    }
    if options.perform_dry_run {
        debug!(
            "Would create symlink: {} -> {}",
            destination.display(),
            link.display()
        );
    } else {
        if let Err(e) = ensure_parent_exists(&destination) {
            error!(
                "Failed to create parent directory: {}, {}",
                destination.display(),
                e
            );
            status.links_errors = 1;
            return status;
        }
        debug!(
            "Creating symlink: {} -> {}",
            destination.display(),
            link.display()
        );
        if let Err(e) = std::os::unix::fs::symlink(&link, &destination) {
            error!("Failed to create symlink: {}, {}", destination.display(), e);
            status.links_errors = 1;
            return status;
        }
    }
    info!(
        "Created symlink: {} -> {}",
        destination.display(),
        link.display()
    );
    status
}

fn sync_file(
    source: &PathBuf,
    destination: &PathBuf,
    options: &SyncOptions,
    metadata: &Metadata,
) -> SyncStatus {
    let mut status = SyncStatus::default();
    let files_differs = files_differs(destination, metadata).unwrap();
    let source_length = metadata.len();
    status.files_total = 1;
    status.bytes_total = source_length;
    if destination.exists() {
        if !destination.is_file() {
            error!(
                "Failed to change: {} of type {:?}",
                destination.display(),
                destination.type_id()
            );
            status.files_errors = 1;
            return status;
        }
        if !files_differs {
            debug!(
                "File up to date, no need to copy: {} -> {}",
                source.display(),
                destination.display()
            );
            return status;
        }
    }

    if options.perform_dry_run {
        debug!(
            "File up to date, no need to copy: {} -> {}",
            source.display(),
            destination.display()
        );
        return status;
    }
    debug!(
        "Copying file: {} -> {}",
        source.display(),
        destination.display()
    );
    if let Err(e) = ensure_parent_exists(&destination) {
        error!(
            "Failed to create parent directory: {}, {}",
            destination.display(),
            e
        );
        status.files_errors = 1;
        return status;
    }
    let bytes_copied = std::fs::copy(&source, &destination);
    if bytes_copied.is_err() {
        error!(
            "Failed to copy file: {} -> {}, {}",
            source.display(),
            destination.display(),
            bytes_copied.unwrap_err()
        );
        status.files_errors = 1;
        return status;
    }
    let bytes_copied = bytes_copied.unwrap();
    info!(
        "Copied file: {} -> {}",
        source.display(),
        destination.display()
    );
    status.bytes_copied = bytes_copied;
    status.files_copied = 1;
    return status;
}

fn ensure_parent_exists(path: &PathBuf) -> Result<(), Error> {
    let path_parent = path.parent().unwrap();
    if !path_parent.exists() {
        warn!("Creating parent directory: {}", path_parent.display());
        std::fs::create_dir_all(&path_parent)?;
    }
    Ok(())
}


fn files_differs(destination: &PathBuf, src_meta: &Metadata) -> Result<bool, Error> {
    if !destination.exists() {
        return Ok(true);
    }

    let dest_meta = destination.metadata()?;

    let src_mtime = FileTime::from_last_modification_time(&src_meta);
    let dest_mtime = FileTime::from_last_modification_time(&dest_meta);

    let src_size = src_meta.len();
    let dest_size = dest_meta.len();

    Ok(src_mtime > dest_mtime || src_size != dest_size)
}


/*
fn copy_file_serial(&self, source: &PathBuf, destination: &PathBuf) -> Result<u64, Error> {
    std::fs::copy(&source, &destination).map_err(|e| {
        Error::new(
            e.kind(),
            format!(
                "Failed to copy file: {} -> {}: {}",
                source.display(),
                destination.display(),
                e
            ),
        )
    })
}

fn copy_file_memmap(source: &PathBuf, destination: &PathBuf) -> Result<u64, Error> {
    let source_file = File::open(&source)?;
    let file_len = source_file.metadata()?.len();
    let destination_file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&destination)?;
    destination_file.set_len(file_len)?;
    let source = unsafe { Mmap::map(&source_file)? };
    let mut dest = unsafe { MmapMut::map_mut(&destination_file)? };

    let pool = ThreadPoolBuilder::new()
        .num_threads(self.parallelism)
        .build()
        .unwrap();

    pool.install(|| {
        dest.par_chunks_mut(CHUNK_SIZE)
            .zip(source.par_chunks(CHUNK_SIZE))
            .for_each(|(dest_chunk, source_chunk)| dest_chunk.copy_from_slice(source_chunk));
    });

    dest.flush()?;

    if dest.len() as u64 != file_len {
        return Err(Error::new(
            std::io::ErrorKind::Other,
            "Copied length mismatch",
        ));
    }

    Ok(file_len)
}

fn copy_file_parallel(&self, source: &PathBuf, destination: &PathBuf) -> Result<u64, Error> {
    let source_file = File::open(&source)?;
    let destination_file = File::create(&destination)?;
    let mut reader = BufReader::with_capacity(BUFFER_SIZE, source_file);
    let mut writer = BufWriter::with_capacity(BUFFER_SIZE, destination_file);

    let mut buffer = vec![0; CHUNK_SIZE];
    let mut total_bytes_copied = 0;
    let source_metadata = source.metadata()?;

    loop {
        let bytes_read = reader.read(&mut buffer)?;
        if bytes_read == 0 {
            break;
        }

        let chunks: Vec<_> = buffer[..bytes_read]
            .par_chunks(CHUNK_SIZE)
            .with_min_len(self.parallelism)
            .map(|chunk| chunk.to_vec())
            .collect();

        for chunk in chunks {
            writer.write_all(&chunk)?;
            let bytes_copied = chunk.len() as u64;
            total_bytes_copied += bytes_copied;
            // debug!("Copied chunk {}/{} from {} to {}", humansize::format_size(total_bytes_copied, humansize::BINARY), humansize::format_size(file_len, humansize::BINARY), source.display(), destination.display());
        }
    }
    writer.flush()?;
    #[cfg(unix)]
    {
        if self.preserve_permissions {
            let permissions = source_metadata.permissions();
            std::fs::set_permissions(&destination, permissions)?;
        }
    }
    Ok(total_bytes_copied)
}

fn parallelism(num_threads: usize) -> Parallelism {
    match num_threads {
        0 => Parallelism::RayonDefaultPool {
            busy_timeout: std::time::Duration::from_secs(1),
        },
        1 => Parallelism::Serial,
        n => Parallelism::RayonExistingPool {
            pool: ThreadPoolBuilder::new()
                .stack_size(128 * 1024)
                .num_threads(n)
                .thread_name(|idx| format!("plsync-thread-{idx}"))
                .build()
                .expect("fields we set cannot fail")
                .into(),
            busy_timeout: None,
        },
    }
}
 */