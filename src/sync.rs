use filetime::set_symlink_file_times;
use filetime::FileTime;
use glob_match::glob_match;
use indicatif::{
    FormattedDuration, HumanBytes, HumanFloatCount, ParallelProgressIterator, ProgressBar,
};
use log::{debug, error, info, warn};
use nix::libc;
use rayon::prelude::*;
use std::any::Any;
use std::fs::Metadata;
use std::io::Error;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::MetadataExt;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::path::PathBuf;

#[derive(Clone, Default, Debug)]
pub struct SyncStatus {
    pub dirs_copied: u64,
    pub dirs_total: u64,
    pub dirs_errors: u64,
    pub dirs_deleted: u64,
    pub files_copied: u64,
    pub files_total: u64,
    pub files_errors: u64,
    pub files_deleted: u64,
    pub links_copied: u64,
    pub links_total: u64,
    pub links_errors: u64,
    pub links_deleted: u64,
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

    pub fn deleted_total(&self) -> u64 {
        self.dirs_deleted + self.files_deleted + self.links_deleted
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

    pub fn bytes_skipped(&self) -> u64 {
        self.bytes_total - self.bytes_copied
    }

    pub fn bandwidth_total(&self, elapsed: &std::time::Duration) -> u64 {
        let elapsed = elapsed.as_secs_f64();
        if elapsed == 0.0 {
            return 0;
        }
        (self.bytes_total as f64 / elapsed) as u64
    }

    pub fn bandwidth_copied(&self, elapsed: &std::time::Duration) -> u64 {
        let elapsed = elapsed.as_secs_f64();
        if elapsed == 0.0 {
            return 0;
        }
        (self.bytes_copied as f64 / elapsed) as u64
    }

    pub fn bandwidth_skipped(&self, elapsed: &std::time::Duration) -> u64 {
        let elapsed = elapsed.as_secs_f64();
        if elapsed == 0.0 {
            return 0;
        }
        (self.bytes_skipped() as f64 / elapsed) as u64
    }

    pub fn merge(&self, other: &Self) -> Self {
        SyncStatus {
            dirs_copied: self.dirs_copied + other.dirs_copied,
            dirs_total: self.dirs_total + other.dirs_total,
            dirs_errors: self.dirs_errors + other.dirs_errors,
            dirs_deleted: self.dirs_deleted + other.dirs_deleted,
            files_copied: self.files_copied + other.files_copied,
            files_total: self.files_total + other.files_total,
            files_errors: self.files_errors + other.files_errors,
            files_deleted: self.files_deleted + other.files_deleted,
            links_copied: self.links_copied + other.links_copied,
            links_total: self.links_total + other.links_total,
            links_errors: self.links_errors + other.links_errors,
            links_deleted: self.links_deleted + other.links_deleted,
            permissions_errors: self.permissions_errors + other.permissions_errors,
            bytes_copied: self.bytes_copied + other.bytes_copied,
            bytes_total: self.bytes_total + other.bytes_total,
        }
    }

    pub fn print(&self) {
        println!(
            "Entries total: {}, copied: {}, skipped: {}, deleted: {}, errors: {}, permission errors: {}",
            HumanFloatCount(self.entries_total() as f64),
            HumanFloatCount(self.copied_total() as f64),
            HumanFloatCount(self.skipped_total() as f64),
            HumanFloatCount(self.deleted_total() as f64),
            HumanFloatCount(self.errors_total() as f64),
            HumanFloatCount(self.permissions_errors as f64)
        );
        println!(
            "Directories total: {}, copied: {}, skipped: {}, deleted: {}, errors: {}",
            HumanFloatCount(self.dirs_total as f64),
            HumanFloatCount(self.dirs_copied as f64),
            HumanFloatCount(self.dirs_skipped() as f64),
            HumanFloatCount(self.dirs_deleted as f64),
            HumanFloatCount(self.dirs_errors as f64)
        );
        println!(
            "Symbolic links total: {}, copied: {}, skipped: {}, deleted: {}, errors: {}",
            HumanFloatCount(self.links_total as f64),
            HumanFloatCount(self.links_copied as f64),
            HumanFloatCount(self.links_skipped() as f64),
            HumanFloatCount(self.links_deleted as f64),
            HumanFloatCount(self.links_errors as f64)
        );
        println!(
            "Files total: {}, copied: {}, skipped: {}, deleted: {}, errors: {}",
            HumanFloatCount(self.files_total as f64),
            HumanFloatCount(self.files_copied as f64),
            HumanFloatCount(self.files_skipped() as f64),
            HumanFloatCount(self.files_deleted as f64),
            HumanFloatCount(self.files_errors as f64)
        );
        println!(
            "Transfered toal: {}, copied {}, skipped: {}",
            HumanBytes(self.bytes_total),
            HumanBytes(self.bytes_copied),
            HumanBytes(self.bytes_skipped()),
        );
    }

    pub fn print_elapsed(&self, start_time: &std::time::Instant) {
        let elapsed = start_time.elapsed();
        println!("Elapsed time: {}", FormattedDuration(elapsed));
        self.print();
        println!(
            "bandwidth toal: {}/s, copied {}/s, skipped: {}/s",
            HumanBytes(self.bandwidth_total(&elapsed)),
            HumanBytes(self.bandwidth_copied(&elapsed)),
            HumanBytes(self.bandwidth_skipped(&elapsed)),
        );
    }
}

pub struct SyncOptions {
    pub preserve_permissions: bool,
    pub perform_dry_run: bool,
    pub delete: bool,
    pub delete_before: bool,
    pub delete_after: bool,
    pub exclude: Vec<String>,
}

impl Default for SyncOptions {
    fn default() -> Self {
        Self {
            preserve_permissions: true,
            perform_dry_run: false,
            delete: false,
            delete_before: false,
            delete_after: false,
            exclude: Vec::new(),
        }
    }
}

pub fn sync(
    source_path: &Path,
    destination_path: &Path,
    options: &SyncOptions,
    progress_bar: &ProgressBar,
) -> SyncStatus {
    if !source_path.is_dir() {
        error!("Source path is not a directory: {}", source_path.display());
        return SyncStatus {
            dirs_errors: 1,
            ..Default::default()
        };
    }
    if !destination_path.is_dir() && !options.perform_dry_run {
        if let Err(e) = std::fs::create_dir_all(destination_path) {
            error!(
                "Failed to create directory: {}, {}",
                destination_path.display(),
                e
            );
            return SyncStatus {
                dirs_errors: 1,
                ..Default::default()
            };
        }
    }
    let mut status = SyncStatus::default();
    if options.delete_before {
        progress_bar.set_message("Delete before phase");
        status = delete_path(destination_path, source_path, options, progress_bar).merge(&status);
        progress_bar.set_message("Copy phase");
        progress_bar.set_position(0);
    }
    if options.delete {
        progress_bar.set_message("Copy and delete phase");
    }
    status = sync_path(source_path, destination_path, options, progress_bar).merge(&status);
    if options.delete_after {
        progress_bar.set_message("Delete after phase");
        progress_bar.set_position(0);
        status = delete_path(destination_path, source_path, options, progress_bar).merge(&status);
    }
    if !options.perform_dry_run {
         if let Err(e) = copy_modification_time(&source_path.metadata().unwrap(), &destination_path.to_path_buf()) {
            error!(
                "Failed to set modification time on dry run: {} -> {}, {}",
                source_path.display(),
                destination_path.display(),
                e
            );
        }
    }
    status
}

fn sync_path(
    source_base: &Path,
    destination_base: &Path,
    options: &SyncOptions,
    progress_bar: &ProgressBar,
) -> SyncStatus {
    let source_dir = std::fs::read_dir(source_base);
    if source_dir.is_err() {
        return SyncStatus {
            permissions_errors: 1,
            ..Default::default()
        };
    }
    source_dir
        .unwrap()
        .par_bridge()
        .progress_with(progress_bar.clone())
        .map(|entry| {
            if entry.is_err() {
                return SyncStatus {
                    permissions_errors: 1,
                    ..Default::default()
                };
            }
            let entry = entry.unwrap();
            let source_path = entry.path();
            let metadata = entry.metadata();
            if metadata.is_err() {
                return SyncStatus {
                    dirs_errors: 1,
                    ..Default::default()
                };
            }
            let metadata = metadata.unwrap();
            let source_relative = source_path.strip_prefix(source_base).unwrap();
            let destination_path = destination_base.join(source_relative);
            if skip_path(&metadata, &source_path, &options.exclude) {
                let mut status = SyncStatus::default();
                match metadata.file_type() {
                    file_type if file_type.is_dir() => status.dirs_total = 1,
                    file_type if file_type.is_symlink() => status.links_total = 1,
                    _ => status.files_total = 1,
                }
                return status;
            }
            if metadata.is_dir() {
                debug!(
                    "Syncing directory: {} -> {}",
                    source_path.display(),
                    destination_path.display()
                );
                let status = sync_dir(&source_path, &destination_path, options, &metadata);
                sync_path(&source_path, &destination_path, options, progress_bar).merge(&status);
                sync_dir_modification_times(
                    &source_path,
                    &destination_path,
                    options,
                    &metadata,
                ).merge(&status)
            } else if metadata.is_symlink() {
                debug!(
                    "Syncing symlink: {} -> {}",
                    source_path.display(),
                    destination_path.display()
                );
                sync_symlink(&source_path, &destination_path, options, &metadata)
            } else {
                debug!(
                    "Syncing file: {} -> {}",
                    source_path.display(),
                    destination_path.display()
                );
                sync_file(&source_path, &destination_path, options, &metadata)
            }
        })
        .reduce(SyncStatus::default, |a, b| a.merge(&b))
}

fn delete_path(
    source_base: &Path,
    destination_base: &Path,
    options: &SyncOptions,
    progress_bar: &ProgressBar,
) -> SyncStatus {
    let source_dir = std::fs::read_dir(source_base);
    if source_dir.is_err() {
        return SyncStatus {
            permissions_errors: 1,
            ..Default::default()
        };
    }
    source_dir
        .unwrap()
        .par_bridge()
        .progress_with(progress_bar.clone())
        .map(|entry| {
            if entry.is_err() {
                return SyncStatus {
                    permissions_errors: 1,
                    ..Default::default()
                };
            }
            let entry = entry.unwrap();
            let source_path = entry.path();
            let metadata = entry.metadata();
            if metadata.is_err() {
                return SyncStatus {
                    dirs_errors: 1,
                    ..Default::default()
                };
            }
            let metadata = metadata.unwrap();
            let source_relative = source_path.strip_prefix(source_base).unwrap();
            let destination_path = destination_base.join(source_relative);
            if skip_path(&metadata, &source_path, &options.exclude) {
                return SyncStatus::default();
            }
            if metadata.is_dir() {
                debug!(
                    "Syncing absent directory: {} -> {}",
                    source_path.display(),
                    destination_path.display()
                );
                let mut status =
                    delete_path(&source_path, &destination_path, options, progress_bar);
                if !destination_path.exists() {
                    status = delete_dir(&source_path, options).merge(&status);
                }
                status
            } else {
                debug!(
                    "Syncing absent file: {} -> {}",
                    source_path.display(),
                    destination_path.display()
                );
                if !destination_path.exists() {
                    delete_file_or_link(&source_path, options, &metadata)
                } else {
                    SyncStatus::default()
                }
            }
        })
        .reduce(SyncStatus::default, |a, b| a.merge(&b))
}

fn skip_path(metadata: &Metadata, source_path: &Path, excluded: &Vec<String>) -> bool {
    let source = source_path.file_name().unwrap().to_string_lossy();

    for pattern in excluded {
        if pattern.is_empty() {
            continue;
        }
        if pattern.as_str() == source {
            debug!(
                "Skipping {} because {} == {}",
                source_path.display(),
                pattern,
                source
            );
            return true;
        }
        if pattern.ends_with('/') && metadata.is_dir() && pattern[..pattern.len() - 1] == source {
            debug!(
                "Skipping dir {} because {} == {}/",
                source_path.display(),
                pattern,
                source
            );
            return true;
        }
        if glob_match(pattern, source_path.to_string_lossy().as_ref())
            || glob_match(pattern, &source)
        {
            debug!(
                "Skipping {} because {} matches {}",
                source_path.display(),
                source,
                pattern
            );
            return true;
        }
    }
    false
}

#[cfg(unix)]
fn copy_permissions(metadata: &Metadata, destination: &PathBuf) -> Result<(), Error> {
    let permissions = metadata.permissions();
    let dest_meta = destination.metadata().unwrap();
    let dest_perm = dest_meta.permissions();
    if permissions != dest_perm {
        debug!(
            "Setting permissions {:o} on {}",
            permissions.mode(),
            destination.display()
        );
        std::fs::set_permissions(destination, permissions)?;
    }
    Ok(())
}

#[cfg(unix)]
fn copy_ownership(metadata: &Metadata, destination: &PathBuf) -> Result<(), Error> {
    #[cfg(unix)]
    {
        if unsafe { libc::geteuid() } == 0 {
            let dest_meta = destination.metadata().unwrap();
            let src_uid = metadata.uid();
            let src_gid = metadata.gid();
            let dest_uid = dest_meta.uid();
            let dest_gid = dest_meta.gid();
            if src_uid != dest_uid || src_gid != dest_gid {
                use std::ffi::CString;
                let c_path = CString::new(destination.as_os_str().as_bytes()).unwrap();
                debug!(
                    "Setting owner {:o} and group {:o} on {}",
                    src_uid,
                    src_gid,
                    destination.display()
                );
                let res = unsafe { libc::chown(c_path.as_ptr(), src_uid, src_gid) };
                if res != 0 {
                    return Err(std::io::Error::last_os_error());
                }
            }
        }
    }
    Ok(())
}

fn copy_modification_time(metadata: &Metadata, destination: &PathBuf) -> Result<(), Error> {
    let src_mtime = FileTime::from_last_modification_time(metadata);
    let dest_meta = destination.metadata().unwrap();
    let dest_mtime = FileTime::from_last_modification_time(&dest_meta);
    if src_mtime != dest_mtime {
        debug!(
            "Setting modification time {:?} on {}",
            src_mtime.unix_seconds(),
            destination.display()
        );
        #[cfg(unix)]
        {
            if destination.symlink_metadata()?.file_type().is_symlink() {
                if let Err(e) = set_symlink_file_times(destination, src_mtime, src_mtime) {
                    error!(
                        "Failed to set symlink modification time on: {}, {}",
                        destination.display(),
                        e
                    );
                    return Err(e);
                }
            } else {
                if let Err(e) = filetime::set_file_mtime(destination, src_mtime) {
                    error!(
                        "Failed to set modification time on: {}, {}",
                        destination.display(),
                        e
                    );
                    return Err(e);
                }
            }
        }
        #[cfg(not(unix))]
        {
            if let Err(e) = filetime::set_file_mtime(destination, src_mtime) {
                error!(
                    "Failed to set modification time on: {}, {}",
                    destination.display(),
                    e
                );
                return Err(e);
            }
        }
    }
    Ok(())
}

fn sync_dir(
    _source: &Path,
    destination: &PathBuf,
    options: &SyncOptions,
    #[cfg(unix)] metadata: &Metadata,
    #[cfg(windows)] _metadata: &Metadata,
) -> SyncStatus {
    let mut status = SyncStatus {
        dirs_total: 1,
        ..Default::default()
    };
    if !destination.exists() {
        if options.perform_dry_run {
            debug!("Would create directory: {}", destination.display());
        } else {
            // assert_parent_exists(&destination)?;
            debug!("Creating directory: {}", destination.display());
            if let Err(e) = ensure_parent_exists(destination) {
                error!(
                    "Failed to create parent directory: {}, {}",
                    destination.display(),
                    e
                );
                status.dirs_errors = 1;
                return status;
            }
            if let Err(e) = std::fs::create_dir(destination) {
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
            if let Err(e) = copy_permissions(metadata, destination) {
                error!(
                    "Failed to set permissions on directory: {}, {}",
                    destination.display(),
                    e
                );
                status.permissions_errors = 1;
            }
        }
    }

    #[cfg(unix)]
    {
        if !options.perform_dry_run && options.preserve_permissions {
            if let Err(e) = copy_ownership(metadata, destination) {
                error!(
                    "Failed to set ownership on directory: {}, {}",
                    destination.display(),
                    e
                );
                status.permissions_errors = 1;
            }
        }
    }
    status
}

fn sync_dir_modification_times(
    _source: &Path,
    destination: &PathBuf,
    options: &SyncOptions,
    #[cfg(unix)] metadata: &Metadata,
    #[cfg(windows)] _metadata: &Metadata,
) -> SyncStatus {
    let mut status = SyncStatus {
        dirs_total: 1,
        ..Default::default()
    };
    if !destination.exists() {
        error!(
            "Failed to create directory: {}",
            destination.display()
        );
        status.dirs_errors = 1;
        return status;
    }

    if !options.perform_dry_run {
        if let Err(e) = copy_modification_time(metadata, destination) {
            error!(
                "Failed to set modification time on directory: {}, {}",
                destination.display(),
                e
            );
            status.dirs_errors = 1;
        }
    }

    status
}

fn delete_dir(source: &PathBuf, options: &SyncOptions) -> SyncStatus {
    let mut status = SyncStatus {
        dirs_deleted: 1,
        ..Default::default()
    };
    if options.perform_dry_run {
        debug!("Would delete directory: {}", source.display());
    } else {
        debug!("Deleting directory: {}", source.display());
        if let Err(e) = std::fs::remove_dir(source) {
            status.dirs_errors = 1;
            error!("Failed to delete directory: {}, {}", source.display(), e);
            return status;
        }
    }
    info!("Deleted directory: {}", source.display());
    status
}

fn sync_symlink(
    source: &PathBuf,
    destination: &PathBuf,
    options: &SyncOptions,
    metadata: &Metadata,
) -> SyncStatus {
    let mut status = SyncStatus {
        links_total: 1,
        ..Default::default()
    };
    let link = std::fs::read_link(source);
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
            let destination_link = std::fs::read_link(destination).unwrap();
            if destination_link != link {
                if options.perform_dry_run {
                    debug!("Would delete existing file: {}", destination.display());
                } else {
                    debug!("Deleting existing file: {}", destination.display());
                    if let Err(e) = std::fs::remove_file(destination) {
                        status.links_errors = 1;
                        error!(
                            "Failed to delete existing file: {}, {}",
                            destination.display(),
                            e
                        );
                        return status;
                    }
                }
                info!("Deleted existing file: {}", destination.display());
            }
        }
    }
    if !destination.exists() {
        if options.perform_dry_run {
            debug!(
                "Would create symlink: {} -> {}",
                destination.display(),
                link.display()
            );
        } else {
            if let Err(e) = ensure_parent_exists(destination) {
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
            #[cfg(unix)]
            {
                if let Err(e) = std::os::unix::fs::symlink(&link, destination) {
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
        }
    }

    #[cfg(unix)]
    {
        if !options.perform_dry_run && options.preserve_permissions {
            if let Err(e) = copy_permissions(metadata, destination) {
                error!(
                    "Failed to set permissions on link: {}, {}",
                    destination.display(),
                    e
                );
                status.permissions_errors = 1;
            }
        }
    }

    #[cfg(unix)]
    {
        if !options.perform_dry_run && options.preserve_permissions {
            if let Err(e) = copy_ownership(metadata, destination) {
                error!(
                    "Failed to set ownership on link: {}, {}",
                    destination.display(),
                    e
                );
                status.permissions_errors = 1;
            }
        }
    }

    if !options.perform_dry_run {
        if let Err(e) = copy_modification_time(metadata, destination) {
            error!(
                "Failed to set modification time on link: {}, {}",
                destination.display(),
                e
            );
            status.links_errors = 1;
        }
    }
    status
}

fn sync_file(
    source: &PathBuf,
    destination: &PathBuf,
    options: &SyncOptions,
    metadata: &Metadata,
) -> SyncStatus {
    let mut status = SyncStatus::default();
    let source_length = metadata.len();
    status.files_total = 1;
    status.bytes_total = source_length;

    let bytes_copied = if options.perform_dry_run {
        debug!(
            "Would copy: {} -> {}",
            source.display(),
            destination.display()
        );
        source_length
    } else {
        let copy_outcome = copy_file_data(source, destination, metadata);
        copy_outcome.unwrap()
    };
    status.bytes_copied = bytes_copied;
    status.files_copied = 1;

    #[cfg(unix)]
    {
        if !options.perform_dry_run && options.preserve_permissions {
            if let Err(e) = copy_permissions(metadata, destination) {
                error!(
                    "Failed to set permissions on file: {}, {}",
                    destination.display(),
                    e
                );
                status.permissions_errors = 1;
            }
        }
    }

    #[cfg(unix)]
    {
        if !options.perform_dry_run && options.preserve_permissions {
            if let Err(e) = copy_ownership(metadata, destination) {
                error!(
                    "Failed to set ownership on file: {}, {}",
                    destination.display(),
                    e
                );
                status.permissions_errors = 1;
            }
        }
    }

    if !options.perform_dry_run {
        if let Err(e) = copy_modification_time(metadata, destination) {
            error!(
                "Failed to set modification time on file: {}, {}",
                destination.display(),
                e
            );
            status.files_errors = 1;
        }
    }

    status
}

fn copy_file_data(
    source: &PathBuf,
    destination: &PathBuf,
    metadata: &Metadata,
) -> Result<u64, Error> {
    if destination.exists() {
        if !destination.is_file() {
            error!(
                "Failed to change: {} of type {:?}",
                destination.display(),
                destination.type_id()
            );
        }
        let src_mtime = FileTime::from_last_modification_time(metadata);
        let dest_meta = destination.metadata()?;
        let dest_mtime = FileTime::from_last_modification_time(&dest_meta);

        let src_size = metadata.len();
        let dest_size = dest_meta.len();

        if src_mtime <= dest_mtime && src_size == dest_size {
            debug!(
                "File is up to date, skipping copy: {} -> {}",
                source.display(),
                destination.display()
            );
            return Ok(0);
        }
    }

    if let Err(e) = ensure_parent_exists(destination) {
        error!(
            "Failed to create parent directory: {}, {}",
            destination.display(),
            e
        );
        return Err(e);
    }
    debug!(
        "Copying file data: {} -> {}",
        source.display(),
        destination.display()
    );
    let bytes_copied = std::fs::copy(source, destination)?;
    info!(
        "Copied file data: {} -> {}",
        source.display(),
        destination.display()
    );
    Ok(bytes_copied)
}

fn delete_file_or_link(source: &PathBuf, options: &SyncOptions, metadata: &Metadata) -> SyncStatus {
    let mut status = SyncStatus::default();
    let file_type = if metadata.is_symlink() {
        "symlink"
    } else {
        "file"
    };
    if metadata.is_symlink() {
        status.links_deleted = 1;
    } else {
        status.files_deleted = 1;
    }
    if options.perform_dry_run {
        debug!("Would delete {}: {}", file_type, source.display());
    } else {
        debug!("Deleting {}: {}", file_type, source.display());
        if let Err(e) = std::fs::remove_file(source) {
            status.links_errors = 1;
            error!(
                "Failed to delete {}: {}, {}",
                file_type,
                source.display(),
                e
            );
            return status;
        }
    }
    info!("Deleted {}: {}", file_type, source.display());
    status
}

fn ensure_parent_exists(path: &Path) -> Result<(), Error> {
    let path_parent = path.parent().unwrap();
    if !path_parent.exists() {
        warn!("Creating parent directory: {}", path_parent.display());
        std::fs::create_dir_all(path_parent)?;
    }
    Ok(())
}
