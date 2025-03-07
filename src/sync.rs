use std::fmt;
use std::fs::OpenOptions;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::path::PathBuf;

use filetime::FileTime;
use jwalk::rayon::iter::IndexedParallelIterator;
use jwalk::rayon::iter::ParallelIterator;
use jwalk::rayon::slice::ParallelSlice;
use jwalk::rayon::ThreadPoolBuilder;
use jwalk::DirEntry;
use jwalk::Parallelism;
use jwalk::WalkDirGeneric;
use log::{debug, info};
use std::fs::File;
use std::io::Error;
use std::io::{BufReader, BufWriter, Read, Write};
use std::time::Duration;

use jwalk::rayon::prelude::*;
use memmap2::{Mmap, MmapMut};

use crate::Throttle;

const BUFFER_SIZE: usize = 128 * 1024;
const CHUNK_SIZE: usize = BUFFER_SIZE * 1024;

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
    pub bytes_copied: u64,
    pub bytes_total: u64,
}

impl SyncStatus {
    pub fn entries_total(&self) -> u64 {
        self.dirs_total + self.files_total + self.links_total
    }

    pub fn errors_total(&self) -> u64 {
        self.dirs_errors + self.files_errors + self.links_errors
    }

    pub fn copied_total(&self) -> u64 {
        self.dirs_copied + self.files_copied + self.links_copied
    }

    pub fn skipped_total(&self) -> u64 {
        self.entries_total() - self.copied_total()
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

type SyncOutcome = Option<Result<SyncResult, Box<dyn std::error::Error + Send>>>;
type SyncDirEntry = DirEntry<((), SyncOutcome)>;
type SyncWalkDir = WalkDirGeneric<((), SyncOutcome)>;

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

    pub fn copy_function(self) -> fn(&SyncOptions, &PathBuf, &PathBuf) -> Result<u64, Error> {
        match self {
            Self::Serial => SyncOptions::copy_file_serial,
            Self::Parallel => SyncOptions::copy_file_parallel,
            Self::Mmap => SyncOptions::copy_file_memmap,
        }
    }
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
    mut err: Option<impl Write>,
    source_path: &Path,
    destination_path: &Path,
    options: SyncOptions,
) -> Result<SyncStatus, Error> {
    let mut status = SyncStatus::default();
    let sync_iterator = options.sync_iterator(source_path, destination_path);
    let progress = Throttle::new(Duration::from_millis(100), None);
    for dir_entry in sync_iterator {
        progress.throttled(|| {
            if let Some(err) = err.as_mut() {
                write!(err, "Syncing {} items, copied {}, skipped {}\r", status.entries_total(), status.copied_total(), status.skipped_total()).ok();
            }
        });
        match dir_entry {
            Ok(dir_entry) => {
                let outcome = &dir_entry.client_state;
                match outcome {
                    Some(Ok(SyncResult::DirSkipped)) => {
                        status.dirs_total += 1;
                    }
                    Some(Ok(SyncResult::DirCopied)) => {
                        status.dirs_total += 1;
                        status.dirs_copied += 1;
                    }
                    Some(Ok(SyncResult::SymLinkSkipped)) => {
                        status.links_total += 1;
                    }
                    Some(Ok(SyncResult::SymLinkCopied)) => {
                        status.links_total += 1;
                        status.links_copied += 1;
                    }
                    Some(Ok(SyncResult::FileSkipped(bytes_skipped))) => {
                        status.files_total += 1;
                        status.bytes_total += bytes_skipped;
                    }
                    Some(Ok(SyncResult::FileCopied(bytes_copied))) => {
                        status.files_total += 1;
                        status.bytes_total += bytes_copied;
                        status.files_copied += 1;
                        status.bytes_copied += bytes_copied;
                    }
                    Some(Err(e)) if e.is::<DirError>() => {
                        status.dirs_total += 1;
                        status.dirs_errors += 1;
                        writeln!(err.as_mut().unwrap(), "{}", e).ok();
                    }
                    Some(Err(e)) if e.is::<SymLinkError>() => {
                        status.links_total += 1;
                        status.links_errors += 1;
                        writeln!(err.as_mut().unwrap(), "{}", e).ok();
                    }
                    Some(Err(e)) if e.is::<FileError>() => {
                        status.files_total += 1;
                        status.files_errors += 1;
                        writeln!(err.as_mut().unwrap(), "{}", e).ok();
                    }
                    Some(Err(e)) => {
                        writeln!(err.as_mut().unwrap(), "{}", e).ok();
                    }
                    None => {}
                }
                // writeln!(out, "Entry: {:?}", dir_entry.path()).ok();
            }
            Err(_) => {}
        }
    }
    Ok(status)
}

impl SyncOptions {
    pub fn sync_iterator(self, source_path: &Path, destination_path: &Path) -> SyncWalkDir {
        let source_base = source_path.to_owned();
        let destination_base = destination_path.to_owned();
        SyncWalkDir::new(source_path)
            .parallelism(self.parallelism())
            .follow_links(false)
            .skip_hidden(false)
            .process_read_dir({
                move |_, _, _, dir_entry_results| {
                    dir_entry_results.iter_mut().for_each(|dir_entry_result| {
                        if let Ok(dir_entry) = dir_entry_result {
                            let source_path_buf = dir_entry.path();
                            let source_relative = source_path_buf
                                .strip_prefix(&*source_base)
                                .unwrap()
                                .to_path_buf();
                            let destination = destination_base.join(&source_relative);
                            dir_entry.client_state = if dir_entry.file_type.is_dir() {
                                self.sync_dir(dir_entry, &destination)
                            } else if dir_entry.file_type.is_symlink() {
                                self.sync_symlink(dir_entry, &destination)
                            } else if dir_entry.file_type.is_file() {
                                self.sync_file(dir_entry, &destination)
                            } else {
                                None
                            };
                        }
                    })
                }
            })
    }

    fn sync_dir(self, dir_entry: &SyncDirEntry, destination: &PathBuf) -> SyncOutcome {
        let mut sync_result = SyncResult::DirSkipped;
        if !destination.exists() {
            if self.perform_dry_run {
                debug!("Would create directory: {}", destination.display());
            } else {
                // assert_parent_exists(&destination)?;
                debug!("Creating directory: {}", destination.display());
                if let Err(e) = std::fs::create_dir_all(&destination) {
                    return Some(Err(Box::new(DirError {
                        path: destination.clone(),
                        error: e,
                    })));
                }
            }
            info!("Created directory: {}", destination.display());
            sync_result = SyncResult::DirCopied;
        }
        #[cfg(unix)]
        {
            if !self.perform_dry_run && self.preserve_permissions {
                if let Err(e) = self.copy_permissions(&dir_entry, destination) {
                    return Some(Err(Box::new(DirError {
                        path: destination.clone(),
                        error: e,
                    })));
                }
            }
        }
        Some(Ok(sync_result))
    }

    fn sync_symlink(self, dir_entry: &SyncDirEntry, destination: &PathBuf) -> SyncOutcome {
        let source = dir_entry.path();
        let link = std::fs::read_link(&source).unwrap();
        let destination_metadata = destination.symlink_metadata();
        if destination_metadata.is_ok() {
            let destination_metadata = destination_metadata.unwrap();
            if destination_metadata.file_type().is_symlink() {
                let destination_link = std::fs::read_link(&destination).unwrap();
                if destination_link == link {
                    return Some(Ok(SyncResult::SymLinkSkipped));
                }
            }
            if self.perform_dry_run {
                debug!("Would delete existing file: {}", destination.display());
            } else {
                debug!("Deleting existing file: {}", destination.display());
                if let Err(e) = std::fs::remove_file(&destination) {
                    return Some(Err(Box::new(SymLinkError {
                        path: destination.clone(),
                        error: e,
                    })));
                }
            }
        }
        if self.perform_dry_run {
            debug!("Would create symlink: {} -> {}", destination.display(), link.display());
        } else {
            if let Err(e) = self.ensure_parent_exists(&destination) {
                return Some(Err(Box::new(SymLinkError {
                    path: destination.clone(),
                    error: e,
                })));
            }
            debug!("Creating symlink: {} -> {}", destination.display(), link.display());
            if let Err(e) = std::os::unix::fs::symlink(&link, &destination) {
                return Some(Err(Box::new(SymLinkError {
                    path: destination.clone(),
                    error: e,
                })));
            }
        }
        info!("Created symlink: {} -> {}", destination.display(), link.display());
        Some(Ok(SyncResult::SymLinkCopied))
    }

    fn sync_file(self, dir_entry: &SyncDirEntry, destination: &PathBuf) -> SyncOutcome {
        let source = dir_entry.path();
        let files_differs = self.files_differs(dir_entry, destination).unwrap();
        let source_length = source.metadata().unwrap().len();
        if !destination.exists() || files_differs {
            if self.perform_dry_run {
                debug!("File up to date, no need to copy: {} -> {}", source.display(), destination.display());
                Some(Ok(SyncResult::FileCopied(source_length)))
            } else {
                debug!("Copying file: {} -> {}", source.display(), destination.display());
                let bytes_copied;
                if let Err(e) = self.ensure_parent_exists(&destination) {
                    return Some(Err(Box::new(FileError {
                        path: destination.clone(),
                        error: e,
                    })));
                }
                let copy_function = self.sync_method.copy_function();
                if source_length as usize > CHUNK_SIZE && self.parallelism > 1 {
                    match copy_function(&self, &source, &destination) {
                        Ok(b) => bytes_copied = b,
                        Err(e) => {
                            return Some(Err(Box::new(FileError {
                                path: destination.clone(),
                                error: e,
                            })))
                        }
                    }
                } else {
                    match self.copy_file_serial(&source, &destination) {
                        Ok(b) => bytes_copied = b,
                        Err(e) => {
                            return Some(Err(Box::new(FileError {
                                path: destination.clone(),
                                error: e,
                            })))
                        }
                    }
                }
                info!("Copied file: {} -> {}", source.display(), destination.display());
                Some(Ok(SyncResult::FileCopied(bytes_copied)))
            }
        } else {
            Some(Ok(SyncResult::FileSkipped(source_length)))
        }
    }

    fn parallelism(self) -> Parallelism {
        match self.parallelism {
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

    fn copy_permissions(self, entry: &SyncDirEntry, destination: &PathBuf) -> Result<(), Error> {
        let metadata = entry.metadata()?;
        let permissions = metadata.permissions();
        debug!("Setting permissions {:o} on {}", permissions.mode(), destination.display());
        std::fs::set_permissions(&destination, permissions)
    }

    fn ensure_parent_exists(self, path: &PathBuf) -> Result<(), Error> {
        let path_parent = path.parent().unwrap();
        if !path_parent.exists() {
            debug!("Creating parent directory: {}", path_parent.display());
            std::fs::create_dir_all(&path_parent)?;
        }
        Ok(())
    }

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

    fn copy_file_memmap(&self, source: &PathBuf, destination: &PathBuf) -> Result<u64, Error> {
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

    fn files_differs(self, dir_entry: &SyncDirEntry, destination: &PathBuf) -> Result<bool, Error> {
        if !destination.exists() {
            return Ok(true);
        }

        let src_meta = dir_entry.metadata()?;
        let dest_meta = destination.metadata()?;

        let src_mtime = FileTime::from_last_modification_time(&src_meta);
        let dest_mtime = FileTime::from_last_modification_time(&dest_meta);

        let src_size = src_meta.len();
        let dest_size = dest_meta.len();

        Ok(src_mtime > dest_mtime || src_size != dest_size)
    }
}
