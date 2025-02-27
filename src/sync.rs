use std::path::Path;
use std::path::PathBuf;
use std::time::Instant;

use filetime::FileTime;
use jwalk::rayon::iter::IndexedParallelIterator;
use jwalk::rayon::iter::ParallelIterator;
use jwalk::rayon::slice::ParallelSlice;
use jwalk::DirEntry;
use jwalk::Parallelism;
use jwalk::WalkDir;
use log::info;
use log::{debug, error, warn};
use std::fs::File;
use std::io::Error;
use std::io::{BufReader, BufWriter, Read, Write};

const BUFFER_SIZE: usize = 8 * 1024;
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

pub struct Sync {
    source: PathBuf,
    destination: PathBuf,
    options: SyncOptions,
}

impl Sync {
    pub fn new(source: &Path, destination: &Path, options: SyncOptions) -> Sync {
        Sync {
            source: source.to_path_buf(),
            destination: destination.to_path_buf(),
            options,
        }
    }

    pub fn parallelism(&self) -> Parallelism {
        match self.options.parallelism {
            1 => Parallelism::Serial,
            n => Parallelism::RayonNewPool(n),
        }
    }

    pub fn sync(&self) -> Result<u64, Error> {
        let mut dirs_copied = 0;
        let mut dirs_total = 0;
        let mut files_copied = 0;
        let mut files_total = 0;
        let mut links_copied = 0;
        let mut links_total = 0;
        let mut errors = 0;
        let mut bytes_copied = 0;
        let mut bytes_total = 0;
        let walk_dir = WalkDir::new(self.source.clone())
            .parallelism(self.parallelism())
            .follow_links(false)
            .skip_hidden(false);
        let now = Instant::now();
        for dir_entry in walk_dir {
            match dir_entry {
                Ok(dir_entry) => {
                    let source_path_buf = dir_entry.path();
                    let source = source_path_buf.strip_prefix(&self.source).unwrap();
                    let destination = self.destination.join(source);
                    if dir_entry.file_type.is_dir() {
                        if let SyncResult::Copied = self.sync_dir(&dir_entry, &destination)? {
                            dirs_copied += 1;
                        }
                        dirs_total += 1;
                    } else if dir_entry.file_type.is_symlink() {
                        if let SyncResult::Copied = self.sync_symlink(&dir_entry, &destination)? {
                            links_copied += 1;
                        }
                        links_total += 1;
                    } else if dir_entry.file_type.is_file() {
                        bytes_total += source_path_buf.metadata()?.len();
                        let file_bytes_copied = self.sync_file(&dir_entry, &destination)?;
                        bytes_copied += file_bytes_copied;
                        if file_bytes_copied > 0 {
                            files_copied += 1;
                        }
                        files_total += 1;
                    } else {
                        warn!("Unknown file type: {:?}", dir_entry.file_type);
                    }
                }
                Err(error) => {
                    error!("Read dir_entry error: {}", error);
                    errors += 1;
                }
            }
            // debug!("{}", entry?.path().display());
        }
        if self.options.show_stats {
            let elapsed = now.elapsed();
            println!("Elapsed time: {}", humantime::format_duration(elapsed));
            println!("Directories: {}/{}", dirs_copied, dirs_total);
            println!("Files: {}/{}", files_copied, files_total);
            println!("Symbolic links: {}/{}", links_copied, links_total);
            println!(
                "Bytes: {}/{}",
                humansize::format_size(bytes_copied, humansize::BINARY),
                humansize::format_size(bytes_total, humansize::BINARY)
            );
            let elapsed_as_seconds = elapsed.as_secs_f32();
            let copied_bandwidth = (bytes_copied as f32 / elapsed_as_seconds) as u64;
            println!(
                "Copied bandwidth: {}/s",
                humansize::format_size(copied_bandwidth, humansize::BINARY),
            );
            let synced_bandwidth = (bytes_total as f32 / elapsed_as_seconds) as u64;
            println!(
                "Synced bandwidth: {}/s",
                humansize::format_size(synced_bandwidth, humansize::BINARY),
            );
            println!("Errors: {}", errors);
        }
        if errors > 0 {
            return Err(Error::new(
                std::io::ErrorKind::Other,
                "Errors occurred during sync",
            ));
        }
        Ok(0)
    }

    pub fn sync_dir(
        &self,
        dir_entry: &DirEntry<((), ())>,
        destination: &PathBuf,
    ) -> Result<SyncResult, Error> {
        let mut outcome = SyncResult::Skipped;
        if !destination.exists() {
            if self.options.perform_dry_run {
                debug!("Would create directory: {}", destination.display());
            } else {
                self.assert_parent_exists(&destination)?;
                debug!("Creating directory: {}", destination.display());
                std::fs::create_dir_all(&destination)?;
            }
            info!("Created directory: {}", destination.display());
            outcome = SyncResult::Copied;
        }
        #[cfg(unix)]
        {
            if !self.options.perform_dry_run && self.options.preserve_permissions {
                self.copy_permissions(&dir_entry, destination)?;
            }
        }
        Ok(outcome)
    }

    pub fn sync_symlink(
        &self,
        dir_entry: &DirEntry<((), ())>,
        destination: &PathBuf,
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
            if self.options.perform_dry_run {
                debug!("Would delete existing file: {}", destination.display(),);
            } else {
                debug!("Deleting existing file: {}", destination.display(),);
                std::fs::remove_file(&destination)?;
            }
        }
        if self.options.perform_dry_run {
            debug!(
                "Would create symlink: {} -> {}",
                destination.display(),
                link.display(),
            );
        } else {
            self.assert_parent_exists(&destination)?;
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
        &self,
        dir_entry: &DirEntry<((), ())>,
        destination: &PathBuf,
    ) -> Result<u64, Error> {
        let source = dir_entry.path();
        let files_differs = self.files_differs(dir_entry, destination)?;
        let mut bytes_copied = 0;
        if !destination.exists() || files_differs {
            let source_length = source.metadata()?.len();
            if self.options.perform_dry_run {
                debug!(
                    "Would copy file: {} -> {}",
                    source.display(),
                    destination.display()
                );
                bytes_copied = source_length;
            } else {
                self.assert_parent_exists(&destination)?;
                debug!(
                    "Copying file: {} -> {}",
                    source.display(),
                    destination.display()
                );
                bytes_copied =
                    if source_length as usize > CHUNK_SIZE && self.options.parallelism > 1 {
                        self.parallel_copy_file(&source, &destination)?
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
            if !self.options.perform_dry_run && self.options.preserve_permissions {
                self.copy_permissions(&dir_entry, &destination)?;
            }
        }
        Ok(bytes_copied)
    }

    pub fn assert_parent_exists(&self, path: &PathBuf) -> Result<(), Error> {
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

    pub fn parallel_copy_file(
        &self,
        source: &PathBuf,
        destination: &PathBuf,
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
                .with_min_len(self.options.parallelism)
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

    pub fn copy_permissions(
        &self,
        entry: &DirEntry<((), ())>,
        destination: &PathBuf,
    ) -> Result<(), Error> {
        let metadata = entry.metadata()?;
        let permissions = metadata.permissions();
        std::fs::set_permissions(&destination, permissions)?;
        Ok(())
    }

    pub fn files_differs(
        &self,
        dir_entry: &DirEntry<((), ())>,
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
}
