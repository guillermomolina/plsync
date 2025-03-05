use std::fs::OpenOptions;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use filetime::FileTime;
use jwalk::rayon::iter::IndexedParallelIterator;
use jwalk::rayon::iter::ParallelIterator;
use jwalk::rayon::slice::ParallelSlice;
use jwalk::rayon::slice::ParallelSliceMut;
use jwalk::DirEntry;
use jwalk::Parallelism;
use jwalk::WalkDirGeneric;
use log::info;
use log::{debug, error, warn};
use std::fs::File;
use std::io::Error;
use std::io::{BufReader, BufWriter, Read, Write};

const BUFFER_SIZE: usize = 128 * 1024;
const CHUNK_SIZE: usize = BUFFER_SIZE * 1024;

#[derive(Copy, Clone)]
pub struct SyncOptions {
    /// Wether to preserve permissions of the source file after the destination is written.
    pub preserve_permissions: bool,
    pub perform_dry_run: bool,
    pub parallelism: usize,
    pub show_progress: bool,
    pub show_stats: bool,
}

impl Default for SyncOptions {
    fn default() -> Self {
        Self {
            preserve_permissions: true,
            perform_dry_run: false,
            parallelism: 1,
            show_progress: false,
            show_stats: false,
        }
    }
}

pub fn parallelism(parallelism: usize) -> Parallelism {
    match parallelism {
        1 => Parallelism::Serial,
        n => Parallelism::RayonNewPool(n),
    }
}

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
    Skipped,
    Copied,
    FileCopied(u64),
}

pub fn sync(source: &Path, destination: &Path, options: SyncOptions) -> Result<u64, Error> {
    let walk_dir = WalkDirGeneric::<(SyncStatus, SyncResult)>::new(source)
        .parallelism(parallelism(options.parallelism))
        .follow_links(false)
        .skip_hidden(false)
        .process_read_dir({
            let source_base = Arc::new(source.to_path_buf());
            let destination_base = Arc::new(destination.to_path_buf());
            let options = Arc::new(options);
            move |_depth, _path, sync_status, dir_entries| {
                process_read_dir(
                    source_base.clone(),
                    destination_base.clone(),
                    options.clone(),
                    sync_status,
                    dir_entries,
                );
            }
        });
    // let start_time = Instant::now();
    // let mut previous_time = start_time;
    // let mut previous_bytes_copied = 0;
    // let mut sync_status = SyncStatus::default();
    let mut finished: u64 = 0;
    for _dir_entry_result in walk_dir {
        finished += 1;
        if finished % 100 == 0 {
            println!("Finished: {}", finished);
        }
        //     let dir_entry = dir_entry_result?;
        //     sync_status = dir_entry::ReadDirState;
        //     if options.show_progress && previous_time.elapsed().as_secs() > 1 {
        //         print_progress(
        //             &sync_status,
        //             start_time,
        //             previous_time,
        //             previous_bytes_copied,
        //         );
        //         previous_bytes_copied = sync_status.bytes_copied;
        //         previous_time = Instant::now();
        //     }
        //     debug!("{:?}", _dir_entry);
    }
    // if options.show_stats {
    //     print_progress(&sync_status, start_time, start_time, 0);
    // }
    // let errors = sync_status.dirs_errors + sync_status.files_errors + sync_status.links_errors;
    // if errors > 0 {
    //     return Err(Error::new(
    //         std::io::ErrorKind::Other,
    //         "Errors occurred during sync",
    //     ));
    // }
    Ok(0)
}

fn print_progress(
    sync_status: &SyncStatus,
    start_time: Instant,
    previous_time: Instant,
    previous_bytes_copied: u64,
) {
    let elapsed = start_time.elapsed();
    println!("\nElapsed time: {}", humantime::format_duration(elapsed));
    println!(
        "Directories: {}/{}",
        sync_status.dirs_copied, sync_status.dirs_total
    );
    println!(
        "Files: {}/{}",
        sync_status.files_copied, sync_status.files_total
    );
    println!(
        "Symbolic links: {}/{}",
        sync_status.links_copied, sync_status.links_total
    );
    println!(
        "Bytes: {}/{}",
        humansize::format_size(sync_status.bytes_copied, humansize::BINARY),
        humansize::format_size(sync_status.bytes_total, humansize::BINARY)
    );
    let elapsed_as_seconds = previous_time.elapsed().as_secs_f32();
    let copied_bandwidth =
        ((sync_status.bytes_copied - previous_bytes_copied) as f32 / elapsed_as_seconds) as u64;
    println!(
        "Copied bandwidth: {}/s",
        humansize::format_size(copied_bandwidth, humansize::BINARY),
    );
    let synced_bandwidth = (sync_status.bytes_total as f32 / elapsed_as_seconds) as u64;
    println!(
        "Synced bandwidth: {}/s",
        humansize::format_size(synced_bandwidth, humansize::BINARY),
    );
    println!(
        "Errors: {}",
        sync_status.dirs_errors + sync_status.files_errors + sync_status.links_errors
    );
}

fn process_read_dir(
    source_base: Arc<PathBuf>,
    destination_base: Arc<PathBuf>,
    options: Arc<SyncOptions>,
    sync_status: &mut SyncStatus,
    dir_entries: &mut Vec<Result<DirEntry<(SyncStatus, SyncResult)>, jwalk::Error>>,
) {
    dir_entries
        .iter_mut()
        .for_each(|dir_entry_result| match dir_entry_result {
            Ok(dir_entry) => {
                let source_path_buf = dir_entry.path();
                let source = source_path_buf
                    .strip_prefix(&*source_base)
                    .unwrap()
                    .to_path_buf();
                let destination = destination_base.join(&source);
                if dir_entry.file_type.is_dir() {
                    sync_status.dirs_total += 1;
                    let sync_result = sync_dir(&dir_entry, &destination, &options);
                    if sync_result.is_ok() {
                        sync_status.dirs_copied += 1;
                        dir_entry.client_state = sync_result.unwrap();
                    } else {
                        sync_status.dirs_errors += 1;
                        error!(
                            "Sync dir: '{}', error: '{}'",
                            destination.display(),
                            sync_result.err().unwrap()
                        );
                    }
                } else if dir_entry.file_type.is_symlink() {
                    sync_status.links_total += 1;
                    let sync_result = sync_symlink(&dir_entry, &destination, &options);
                    if sync_result.is_ok() {
                        sync_status.links_copied += 1;
                        dir_entry.client_state = sync_result.unwrap();
                    } else {
                        sync_status.links_errors += 1;
                        error!(
                            "Sync symlink: '{}', error: '{}'",
                            destination.display(),
                            sync_result.err().unwrap()
                        );
                    }
                } else if dir_entry.file_type.is_file() {
                    sync_status.files_total += 1;
                    let sync_result = sync_file(&dir_entry, &destination, &options);
                    if sync_result.is_ok() {
                        dir_entry.client_state = sync_result.unwrap();
                        match dir_entry.client_state {
                            SyncResult::FileCopied(bytes_copied) => {
                                sync_status.files_copied += 1;
                                sync_status.bytes_copied += bytes_copied;
                            }
                            SyncResult::Skipped => {}
                            _ => {}
                        }
                    } else {
                        sync_status.files_errors += 1;
                        error!(
                            "Sync file: '{}', error: '{}'",
                            destination.display(),
                            sync_result.err().unwrap()
                        );
                    }
                    let processed = sync_status.dirs_total + sync_status.links_total + sync_status.files_total;
                    // if (processed % 100 >= 0) && processed % 100 < 10 {
                        println!("Processed: {}", processed);
                    // }
                } else {
                    warn!("Unknown file type: {:?}", dir_entry.file_type);
                }
            }
            Err(error) => {
                error!("Read dir_entry error: {}", error);
            }
        })
}

pub fn sync_dir(
    dir_entry: &DirEntry<(SyncStatus, SyncResult)>,
    destination: &PathBuf,
    options: &SyncOptions,
) -> Result<SyncResult, Error> {
    let mut sync_result = SyncResult::Skipped;
    if !destination.exists() {
        if options.perform_dry_run {
            debug!("Would create directory: {}", destination.display());
        } else {
            // assert_parent_exists(&destination)?;
            debug!("Creating directory: {}", destination.display());
            std::fs::create_dir_all(&destination)?;
        }
        info!("Created directory: {}", destination.display());
        sync_result = SyncResult::Copied;
    }
    #[cfg(unix)]
    {
        if !options.perform_dry_run && options.preserve_permissions {
            copy_permissions(&dir_entry, destination)?;
        }
    }
    Ok(sync_result)
}

pub fn sync_symlink(
    dir_entry: &DirEntry<(SyncStatus, SyncResult)>,
    destination: &PathBuf,
    options: &SyncOptions,
) -> Result<SyncResult, Error> {
    let source = dir_entry.path();
    let link = std::fs::read_link(&source)?;
    let destination_metadata = destination.symlink_metadata();
    if destination_metadata.is_ok() {
        let destination_metadata = destination_metadata.unwrap();
        if destination_metadata.file_type().is_symlink() {
            let destination_link = std::fs::read_link(&destination)?;
            if destination_link == link {
                return Ok(SyncResult::Skipped);
            }
        }
        if options.perform_dry_run {
            debug!("Would delete existing file: {}", destination.display(),);
        } else {
            debug!("Deleting existing file: {}", destination.display(),);
            std::fs::remove_file(&destination)?;
        }
    }
    if options.perform_dry_run {
        debug!(
            "Would create symlink: {} -> {}",
            destination.display(),
            link.display(),
        );
    } else {
        ensure_parent_exists(&destination)?;
        debug!(
            "Creating symlink: {} -> {}",
            destination.display(),
            link.display(),
        );
        std::os::unix::fs::symlink(&link, &destination)?;
    }
    info!(
        "Created symlink: {} -> {}",
        destination.display(),
        link.display(),
    );
    Ok(SyncResult::Copied)
}

pub fn sync_file(
    dir_entry: &DirEntry<(SyncStatus, SyncResult)>,
    destination: &PathBuf,
    options: &SyncOptions,
) -> Result<SyncResult, Error> {
    let source = dir_entry.path();
    let files_differs = files_differs(dir_entry, destination)?;
    if !destination.exists() || files_differs {
        let source_length = source.metadata()?.len();
        let bytes_copied;
        if options.perform_dry_run {
            debug!(
                "Would copy file: {} -> {}",
                source.display(),
                destination.display()
            );
            bytes_copied = source_length;
        } else {
            ensure_parent_exists(&destination)?;
            debug!(
                "Copying file: {} -> {}",
                source.display(),
                destination.display()
            );
            bytes_copied = if source_length as usize > CHUNK_SIZE && options.parallelism > 1 {
                parallel_copy_file(&source, &destination, &options)?
            } else {
                std::fs::copy(&source, &destination)?
            };
            info!(
                "Copied file: {} -> {}",
                source.display(),
                destination.display()
            );
        }
        Ok(SyncResult::FileCopied(bytes_copied))
    } else {
        debug!(
            "File up to date, no need to copy: {} -> {}",
            source.display(),
            destination.display()
        );
        Ok(SyncResult::Skipped)
    }
}

pub fn assert_parent_exists(path: &PathBuf) -> Result<(), Error> {
    let path_parent = path.parent().unwrap();
    if !path_parent.exists() {
        error!("Parent directory does not exist: {}", path_parent.display());
        Err(Error::new(
            std::io::ErrorKind::NotFound,
            "Parent directory does not exist",
        ))?;
    }
    Ok(())
}

pub fn ensure_parent_exists(path: &PathBuf) -> Result<(), Error> {
    let path_parent = path.parent().unwrap();
    if !path_parent.exists() {
        debug!("Creating parent directory: {}", path_parent.display());
        std::fs::create_dir_all(&path_parent)?;
    }
    Ok(())
}

pub fn parallel_copy_file_mmap(
    source: &PathBuf,
    destination: &PathBuf,
    _options: &SyncOptions,
) -> Result<u64, Error> {
    let source_file = File::open(&source)?;
    let file_len = source_file.metadata()?.len();
    let destination_file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(&destination)?;
    destination_file.set_len(file_len)?;
    let source = unsafe { memmap::Mmap::map(&source_file)? };
    let mut dest = unsafe { memmap::MmapMut::map_mut(&destination_file)? };

    dest.par_chunks_mut(CHUNK_SIZE)
        .zip(source.par_chunks(CHUNK_SIZE))
        .for_each(|(dest_chunk, source_chunk)| dest_chunk.copy_from_slice(source_chunk));

    Ok(file_len)
}

pub fn parallel_copy_file(
    source: &PathBuf,
    destination: &PathBuf,
    options: &SyncOptions,
) -> Result<u64, Error> {
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
            .with_min_len(options.parallelism)
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
        if options.preserve_permissions {
            let permissions = source_metadata.permissions();
            std::fs::set_permissions(&destination, permissions)?;
        }
    }
    Ok(total_bytes_copied)
}

pub fn copy_permissions(
    entry: &DirEntry<(SyncStatus, SyncResult)>,
    destination: &PathBuf,
) -> Result<(), Error> {
    let metadata = entry.metadata()?;
    let permissions = metadata.permissions();
    std::fs::set_permissions(&destination, permissions)?;
    Ok(())
}

pub fn files_differs(
    dir_entry: &DirEntry<(SyncStatus, SyncResult)>,
    destination: &PathBuf,
) -> Result<bool, Error> {
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
