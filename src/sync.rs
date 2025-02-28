use std::fs::OpenOptions;
use std::path::Path;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use filetime::FileTime;
use jwalk::rayon::iter::IndexedParallelIterator;
use jwalk::rayon::iter::ParallelIterator;
use jwalk::rayon::slice::ParallelSlice;
use jwalk::rayon::slice::ParallelSliceMut;
use jwalk::DirEntry;
use jwalk::Parallelism;
use jwalk::WalkDir;
use log::info;
use log::{debug, error, warn};
use std::fs::File;
use std::io::Error;
use std::io::{BufReader, BufWriter, Read, Write};

const BUFFER_SIZE: usize = 128 * 1024;
const CHUNK_SIZE: usize = BUFFER_SIZE * 1024;

pub enum SyncResult {
    Copied,
    Skipped,
}

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

struct SyncStatus {
    pub source: PathBuf,
    pub destination: PathBuf,
    pub options: SyncOptions,
    pub dirs_copied: u64,
    pub dirs_total: u64,
    pub files_copied: u64,
    pub files_total: u64,
    pub links_copied: u64,
    pub links_total: u64,
    pub errors: u64,
    pub bytes_copied: u64,
    pub bytes_total: u64,
}

impl SyncStatus {
    fn new(source: PathBuf, destination: PathBuf, options: SyncOptions) -> Self {
        Self {
            source,
            destination,
            options,
            dirs_copied: 0,
            dirs_total: 0,
            files_copied: 0,
            files_total: 0,
            links_copied: 0,
            links_total: 0,
            errors: 0,
            bytes_copied: 0,
            bytes_total: 0,
        }
    }
}

pub fn sync(source: &Path, destination: &Path, options: SyncOptions) -> Result<u64, Error> {
    let sync_status = Arc::new(Mutex::new(SyncStatus::new(
        source.to_path_buf(),
        destination.to_path_buf(),
        options,
    )));
    let walk_dir = WalkDir::new(source)
        .parallelism(parallelism(options.parallelism))
        .follow_links(false)
        .skip_hidden(false)
        .process_read_dir({
            let sync_status = Arc::clone(&sync_status);
            move |_, _, _, dir_entry_results| {
                process_read_dir(sync_status.clone(), dir_entry_results);
            }
        });
    let start_time = Instant::now();
    let mut previous_time = start_time;
    let mut previous_bytes_copied = 0;
    for dir_entry in walk_dir {
        match dir_entry {
            Ok(dir_entry) => {
                debug!("{}", dir_entry.path().display());
            }
            Err(error) => {
                sync_status.lock().unwrap().errors += 1;
                error!("Walk dir_entry error: {}", error);
            }
        }
        if options.show_progress && previous_time.elapsed().as_secs() > 1 {
            print_progress(
                &sync_status,
                start_time,
                previous_time,
                previous_bytes_copied,
            );
            previous_bytes_copied = sync_status.lock().unwrap().bytes_copied;
            previous_time = Instant::now();
        }
        // debug!("{}", entry?.path().display());
    }
    if options.show_stats {
        print_progress(&sync_status, start_time, start_time, 0);
    }
    if sync_status.lock().unwrap().errors > 0 {
        return Err(Error::new(
            std::io::ErrorKind::Other,
            "Errors occurred during sync",
        ));
    }
    Ok(0)
}

fn print_progress(
    sync_status: &Arc<Mutex<SyncStatus>>,
    start_time: Instant,
    previous_time: Instant,
    previous_bytes_copied: u64,
) {
    let elapsed = start_time.elapsed();
    let synck_status_lckd = sync_status.lock().unwrap();
    println!("\nElapsed time: {}", humantime::format_duration(elapsed));
    println!(
        "Directories: {}/{}",
        synck_status_lckd.dirs_copied, synck_status_lckd.dirs_total
    );
    println!(
        "Files: {}/{}",
        synck_status_lckd.files_copied, synck_status_lckd.files_total
    );
    println!(
        "Symbolic links: {}/{}",
        synck_status_lckd.links_copied, synck_status_lckd.links_total
    );
    println!(
        "Bytes: {}/{}",
        humansize::format_size(synck_status_lckd.bytes_copied, humansize::BINARY),
        humansize::format_size(synck_status_lckd.bytes_total, humansize::BINARY)
    );
    let elapsed_as_seconds = previous_time.elapsed().as_secs_f32();
    let copied_bandwidth = ((synck_status_lckd.bytes_copied - previous_bytes_copied) as f32
        / elapsed_as_seconds) as u64;
    println!(
        "Copied bandwidth: {}/s",
        humansize::format_size(copied_bandwidth, humansize::BINARY),
    );
    let synced_bandwidth = (synck_status_lckd.bytes_total as f32 / elapsed_as_seconds) as u64;
    println!(
        "Synced bandwidth: {}/s",
        humansize::format_size(synced_bandwidth, humansize::BINARY),
    );
    println!("Errors: {}", synck_status_lckd.errors);
}

fn process_read_dir(
    sync_status: Arc<Mutex<SyncStatus>>,
    dir_entry_results: &mut Vec<Result<DirEntry<((), ())>, jwalk::Error>>,
) {
    dir_entry_results
        .iter_mut()
        .for_each(|dir_entry_result| match dir_entry_result {
            Ok(dir_entry) => {
                let options = sync_status.lock().unwrap().options;
                let source_path_buf = dir_entry.path();
                let source = source_path_buf
                    .strip_prefix(&sync_status.lock().unwrap().source)
                    .unwrap()
                    .to_path_buf();
                let destination = sync_status.lock().unwrap().destination.join(&source);
                if dir_entry.file_type.is_dir() {
                    if let SyncResult::Copied =
                        sync_dir(&dir_entry, &destination, &options).unwrap()
                    {
                        sync_status.lock().unwrap().dirs_copied += 1;
                    }
                    sync_status.lock().unwrap().dirs_total += 1;
                } else if dir_entry.file_type.is_symlink() {
                    if let SyncResult::Copied =
                        sync_symlink(&dir_entry, &destination, &options).unwrap()
                    {
                        sync_status.lock().unwrap().links_copied += 1;
                    }
                    sync_status.lock().unwrap().links_total += 1;
                } else if dir_entry.file_type.is_file() {
                    if source_path_buf.metadata().is_ok() { 
                        sync_status.lock().unwrap().bytes_total +=
                            source_path_buf.metadata().unwrap().len();
                        let file_bytes_copied = sync_file(&dir_entry, &destination, &options).unwrap();
                        sync_status.lock().unwrap().bytes_copied += file_bytes_copied;
                        if file_bytes_copied > 0 {
                            sync_status.lock().unwrap().files_copied += 1;
                        }
                        sync_status.lock().unwrap().files_total += 1; 
                    } else {
                        warn!("Could not read metadata for file: {:?}", source_path_buf);
                    }
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
    dir_entry: &DirEntry<((), ())>,
    destination: &PathBuf,
    options: &SyncOptions,
) -> Result<SyncResult, Error> {
    let mut outcome = SyncResult::Skipped;
    if !destination.exists() {
        if options.perform_dry_run {
            debug!("Would create directory: {}", destination.display());
        } else {
            // assert_parent_exists(&destination)?;
            debug!("Creating directory: {}", destination.display());
            std::fs::create_dir_all(&destination)?;
        }
        info!("Created directory: {}", destination.display());
        outcome = SyncResult::Copied;
    }
    #[cfg(unix)]
    {
        if !options.perform_dry_run && options.preserve_permissions {
            copy_permissions(&dir_entry, destination)?;
        }
    }
    Ok(outcome)
}

pub fn sync_symlink(
    dir_entry: &DirEntry<((), ())>,
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
    dir_entry: &DirEntry<((), ())>,
    destination: &PathBuf,
    options: &SyncOptions,
) -> Result<u64, Error> {
    let source = dir_entry.path();
    let files_differs = files_differs(dir_entry, destination)?;
    let mut bytes_copied = 0;
    if !destination.exists() || files_differs {
        let source_length = source.metadata()?.len();
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
            }
        }
    }
    info!(
        "Copied file: {} -> {}",
        source.display(),
        destination.display()
    );
    #[cfg(unix)]
    {
        if !options.perform_dry_run && options.preserve_permissions {
            copy_permissions(&dir_entry, &destination)?;
        }
    }
    Ok(bytes_copied)
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
    let file_len = source.metadata()?.len();

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
    Ok(total_bytes_copied)
}

pub fn copy_permissions(entry: &DirEntry<((), ())>, destination: &PathBuf) -> Result<(), Error> {
    let metadata = entry.metadata()?;
    let permissions = metadata.permissions();
    std::fs::set_permissions(&destination, permissions)?;
    Ok(())
}

pub fn files_differs(dir_entry: &DirEntry<((), ())>, destination: &PathBuf) -> Result<bool, Error> {
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
