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
use log::{debug, info, error};
use std::fs::File;
use std::io::Error;
use std::io::{BufReader, BufWriter, Read, Write};
use std::time::Duration;

use crate::Throttle;

const BUFFER_SIZE: usize = 128 * 1024;
const CHUNK_SIZE: usize = BUFFER_SIZE * 1024;

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

type SyncOutcome = Option<Result<SyncResult, Error>>;
type SyncDirEntry = DirEntry<((), SyncOutcome)>;
type SyncWalkDir = WalkDirGeneric<((), SyncOutcome)>;

#[derive(Copy, Clone)]
pub struct SyncOptions {
    /// Wether to preserve permissions of the source file after the destination is written.
    pub preserve_permissions: bool,
    pub perform_dry_run: bool,
    pub parallelism: usize,
}

impl Default for SyncOptions {
    fn default() -> Self {
        Self {
            preserve_permissions: true,
            perform_dry_run: false,
            parallelism: 1,
        }
    }
}

pub fn sync(
    mut out: impl Write,
    mut err: Option<impl Write>,
    source_path: &Path,
    destination_path: &Path,
    options: SyncOptions,
) -> Result<SyncStatus, Error> {
    let mut status = SyncStatus::default();
    let sync_iterator = options.sync_iterator(source_path, destination_path);
    let progress = Throttle::new(Duration::from_millis(100), Duration::from_secs(1).into());
    let mut entries_traversed: u64 = 0;
    for dir_entry in sync_iterator {
        progress.throttled(|| {
            if let Some(err) = err.as_mut() {
                entries_traversed += 1;
                write!(err, "Enumerating {} items\r", entries_traversed).ok();
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
                    Some(Err(_)) => {
                    }
                    None => {}
                }
                writeln!(out, "Entry: {:?}", dir_entry.path()).ok();
            }
            Err(_) => {
            }
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
                    error!("Failed to create directory: {}: {}", destination.display(), e);
                }
            }
            info!("Created directory: {}", destination.display());
            sync_result = SyncResult::DirCopied;
        }
        #[cfg(unix)]
        {
            if !self.perform_dry_run && self.preserve_permissions {
                if let Err(e) = self.copy_permissions(&dir_entry, destination) {
                    error!("Failed to copy permissions: {}", e);
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
                    error!("Failed to delete existing file: {}: {}", destination.display(), e);
                };
            }
        }
        if self.perform_dry_run {
            debug!(
                "Would create symlink: {} -> {}",
                destination.display(),
                link.display(),
            );
        } else {
            if let Err(e) = self.ensure_parent_exists(&destination) {
                error!("Failed to ensure parent exists: {}", e);
            }
            debug!(
                "Creating symlink: {} -> {}",
                destination.display(),
                link.display(),
            );
            if let Err(e) = std::os::unix::fs::symlink(&link, &destination) {
                error!("Failed to create symlink: {} -> {}: {}", link.display(), destination.display(), e);
            }
        }
        info!(
            "Created symlink: {} -> {}",
            destination.display(),
            link.display(),
        );
        Some(Ok(SyncResult::SymLinkCopied))
    }

    fn sync_file(self, dir_entry: &SyncDirEntry, destination: &PathBuf) -> SyncOutcome {
        let source = dir_entry.path();
        let files_differs = self.files_differs(dir_entry, destination).unwrap();
        let source_length = source.metadata().unwrap().len();
        if !destination.exists() || files_differs {
            let bytes_copied;
            if self.perform_dry_run {
                debug!(
                    "Would copy file: {} -> {}",
                    source.display(),
                    destination.display()
                );
                bytes_copied = source_length;
            } else {
                debug!(
                    "Copying file: {} -> {}",
                    source.display(),
                    destination.display()
                );
                if let Err(e) = self.ensure_parent_exists(&destination) {
                    error!("Failed to ensure parent exists: {}", e);
                }
                bytes_copied = if source_length as usize > CHUNK_SIZE && self.parallelism > 1 {
                    self.parallel_copy_file(&source, &destination).unwrap()
                } else {
                    std::fs::copy(&source, &destination).unwrap()
                };
                info!(
                    "Copied file: {} -> {}",
                    source.display(),
                    destination.display()
                );
            }
            Some(Ok(SyncResult::FileCopied(bytes_copied)))
        } else {
            debug!(
                "File up to date, no need to copy: {} -> {}",
                source.display(),
                destination.display()
            );
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
                    .thread_name(|idx| format!("sync-thread-{idx}"))
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
        debug!(
            "Setting permissions {:o} on {}",
            permissions.mode(),
            destination.display()
        );
        std::fs::set_permissions(&destination, permissions)?;
        Ok(())
    }

    fn ensure_parent_exists(self, path: &PathBuf) -> Result<(), Error> {
        let path_parent = path.parent().unwrap();
        if !path_parent.exists() {
            debug!("Creating parent directory: {}", path_parent.display());
            std::fs::create_dir_all(&path_parent)?;
        }
        Ok(())
    }

    // fn parallel_copy_file_mmap(
    //     self,
    //     source: &PathBuf,
    //     destination: &PathBuf,
    // ) -> Result<u64, Error> {
    //     let source_file = File::open(&source)?;
    //     let file_len = source_file.metadata()?.len();
    //     let destination_file = OpenOptions::new()
    //         .read(true)
    //         .write(true)
    //         .create(true)
    //         .open(&destination)?;
    //     destination_file.set_len(file_len)?;
    //     let source = memmap2::Mmap::map(&source_file)?;
    //     let mut dest = memmap2::MmapMut::map_mut(&destination_file)?;

    //     dest.par_chunks_mut(CHUNK_SIZE)
    //         .zip(source.par_chunks(CHUNK_SIZE))
    //         .for_each(|(dest_chunk, source_chunk)| dest_chunk.copy_from_slice(source_chunk));

    //     Ok(file_len)
    // }

    fn parallel_copy_file(self, source: &PathBuf, destination: &PathBuf) -> Result<u64, Error> {
        let source_file = File::open(&source)?;
        let destination_file = File::create(&destination)?;
        let mut reader = BufReader::with_capacity(BUFFER_SIZE, source_file);
        let mut writer = BufWriter::with_capacity(BUFFER_SIZE, destination_file);

        let mut buffer = vec![0; CHUNK_SIZE];
        let mut total_bytes_copied = 0;
        let source_metadata = source.metadata()?;
        let file_len = source_metadata.len();

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
                debug!(
                    "Copied chunk {}/{} from {} to {}",
                    humansize::format_size(total_bytes_copied, humansize::BINARY),
                    humansize::format_size(file_len, humansize::BINARY),
                    source.display(),
                    destination.display()
                );
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
