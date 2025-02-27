use std::path::Path;
use std::path::PathBuf;

use jwalk::DirEntry;
use jwalk::Parallelism;
use jwalk::WalkDir;
use log::debug;
use log::warn;
use filetime::FileTime;
use std::io::Error;

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
        let mut dirs = 0;
        let mut files = 0;
        let mut symlinks = 0;
        let walk_dir = WalkDir::new(self.source.clone())
            .parallelism(self.parallelism())
            .follow_links(false)
            .skip_hidden(false);
        for dir_entry in walk_dir {
            match dir_entry {
                Ok(dir_entry) => {
                    let source_path_buf = dir_entry.path();
                    let source = source_path_buf.strip_prefix(&self.source).unwrap();
                    let destination = self.destination.join(source);
                    if dir_entry.file_type.is_dir() {
                        self.sync_dir(&destination)?;
                        dirs += 1;
                    } else if dir_entry.file_type.is_file() {
                        self.sync_file(&dir_entry, &destination)?;
                        files += 1;
                    } else if dir_entry.file_type.is_symlink() {
                        self.sync_symlink(&dir_entry, &destination)?;
                        symlinks += 1
                    } else {
                        warn!("Unknown file type: {:?}", dir_entry.file_type);
                    }
                    #[cfg(unix)]
                    {
                        if !self.options.perform_dry_run && self.options.preserve_permissions {
                            self.copy_permissions(&dir_entry, destination)?;
                        }
                    }
                }
                Err(error) => {
                    println!("Read dir_entry error: {}", error);
                }
            }
            // debug!("{}", entry?.path().display());
        }
        println!("Directories: {}", dirs);
        println!("Files: {}", files);
        println!("Symbolic links: {}", symlinks);
        Ok(0)
    }

    pub fn sync_dir(&self, destination: &Path) -> Result<(), Error> {
        if !destination.exists() {
            if self.options.perform_dry_run {
                debug!("Would create directory: {}", destination.display());
            } else {
                debug!("Creating directory: {}", destination.display());
                std::fs::create_dir_all(&destination)?;
            }
        }
        Ok(())
    }

    pub fn sync_symlink(
        &self,
        dir_entry: &DirEntry<((), ())>,
        destination: &PathBuf,
    ) -> Result<(), Error> {
        let source = dir_entry.path();
        let link = std::fs::read_link(&source)?;
        let destination_parent = destination.parent().unwrap();
        if !destination_parent.exists() {
            self.sync_dir(destination_parent)?;
        }
        if !destination.exists() {
            if self.options.perform_dry_run {
                debug!(
                    "Would create symlink: {} -> {}",
                    source.display(),
                    destination.display()
                );
            } else {
                debug!(
                    "Creating symlink: {} -> {}",
                    source.display(),
                    destination.display()
                );
                std::os::unix::fs::symlink(&link, &destination)?;
            }
        }
        Ok(())
    }

    pub fn sync_file(
        &self,
        dir_entry: &DirEntry<((), ())>,
        destination: &PathBuf,
    ) -> Result<(), Error> {
        let source = dir_entry.path();
        let files_differs = self.files_differs(dir_entry, destination)?;
        if !destination.exists() || files_differs {
            if self.options.perform_dry_run {
                debug!(
                    "Would copy file: {} -> {}",
                    source.display(),
                    destination.display()
                );
            } else {
                debug!(
                    "Copying file: {} -> {}",
                    source.display(),
                    destination.display()
                );
                std::fs::copy(&source, &destination)?;
            }
        }
        Ok(())
    }

    pub fn copy_permissions(
        &self,
        entry: &DirEntry<((), ())>,
        destination: PathBuf,
    ) -> Result<(), Error> {
        let metadata = entry.metadata()?;
        let permissions = metadata.permissions();
        std::fs::set_permissions(&destination, permissions)?;
        Ok(())
    }

    pub fn files_differs(&self, dir_entry: &DirEntry<((), ())>, destination: &PathBuf) -> Result<bool, Error> {
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
