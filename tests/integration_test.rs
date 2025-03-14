use std::fs;
use std::io;
// use std::io;
#[cfg(unix)]
use std::os::unix;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::path::PathBuf;
use std::process::Command;

use filetime::FileTime;
use indicatif::{ProgressBar, ProgressDrawTarget};
use plsync::set_thread_pool;
// use filetime::FileTime;
use tempfile::TempDir;

fn assert_same_contents(a: &Path, b: &Path) {
    assert!(a.exists(), "{:?} does not exist", a);
    assert!(b.exists(), "{:?} does not exist", b);
    let status = Command::new("diff")
        .args([a, b])
        .status()
        .expect("Failed to execute process");
    assert!(status.success(), "{:?} and {:?} differ", a, b)
}

#[cfg(unix)]
fn is_executable(path: &Path) -> bool {
    let metadata = std::fs::metadata(path)
        .unwrap_or_else(|e| panic!("Could not get metadata of {:?}: {}", path, e));
    let permissions = metadata.permissions();
    let mode = permissions.mode();
    mode & 0o111 != 0
}

#[cfg(unix)]
fn assert_executable(path: &Path) {
    assert!(
        is_executable(path),
        "{:?} does not appear to be executable",
        path
    );
}

// #[cfg(unix)]
// fn assert_not_executable(path: &Path) {
//     assert!(!is_executable(path), "{:?} appears to be executable", path);
// }

fn setup_test(tmp_path: &Path) -> (PathBuf, PathBuf) {
    let src_path = tmp_path.join("src");
    let dest_path = tmp_path.join("dest");
    let status = Command::new("cp")
        .args(["-R", "tests/data", &src_path.to_string_lossy()])
        .status()
        .expect("Failed to start cp process");
    assert!(status.success(), "could not copy test data");
    let _ = set_thread_pool(1);
    (src_path, dest_path)
}

fn make_recent(path: &Path) -> io::Result<()> {
    let metadata = fs::metadata(path)?;
    let atime = FileTime::from_last_access_time(&metadata);
    let mtime = FileTime::from_last_modification_time(&metadata);
    let mut epoch = mtime.unix_seconds();
    epoch += 1;
    let mtime = FileTime::from_unix_time(epoch, 0);
    filetime::set_file_times(path, atime, mtime)?;
    Ok(())
}

fn new_sync_options() -> plsync::SyncOptions {
    let sync_options = plsync::SyncOptions::default();
    sync_options
}

fn new_progress_bar() -> ProgressBar {
    let progress_bar = ProgressBar::new(0);
    progress_bar.set_draw_target(ProgressDrawTarget::hidden());
    progress_bar
}

#[test]
fn fresh_copy() -> Result<(), std::io::Error> {
    let tmp_dir = TempDir::new()?;
    let (src_path, dest_path) = setup_test(tmp_dir.path());
    let options = new_sync_options();
    let progress_bar = new_progress_bar();
    let stats = plsync::sync(&src_path, &dest_path, &options, &progress_bar);
    assert!(stats.errors_total() == 0);

    let src_top = src_path.join("top.txt");
    let dest_top = dest_path.join("top.txt");
    assert_same_contents(&src_top, &dest_top);

    Ok(())
}

#[test]
fn skip_up_to_date_files() -> Result<(), std::io::Error> {
    let tmp_dir = TempDir::new()?;
    let (src_path, dest_path) = setup_test(tmp_dir.path());
    let options = new_sync_options();
    let progress_bar = new_progress_bar();
    let stats = plsync::sync(&src_path, &dest_path, &options, &progress_bar);
    assert_eq!(stats.skipped_total(), 0);

    let src_top_txt = src_path.join("top.txt");
    make_recent(&src_top_txt)?;
    let stats = plsync::sync(&src_path, &dest_path, &options, &progress_bar);
    assert_eq!(stats.files_copied, 1);

    Ok(())
}

#[test]
#[cfg(unix)]
fn preserve_permissions() -> Result<(), std::io::Error> {
    let tmp_dir = TempDir::new()?;
    let (src_path, dest_path) = setup_test(tmp_dir.path());
    let options = new_sync_options();
    let progress_bar = new_progress_bar();
    plsync::sync(&src_path, &dest_path, &options, &progress_bar);

    let dest_exe = &dest_path.join("a_dir/foo.exe");
    assert_executable(dest_exe);
    Ok(())
}

// #[test]
// #[cfg(unix)]
// fn do_not_preserve_permissions() -> Result<(), std::io::Error> {
//     let tmp_dir = TempDir::new()?;
//     let (src_path, dest_path) = setup_test(tmp_dir.path());
//     let mut options = new_sync_options();
//     options.preserve_permissions = false;
//     let progress_bar = new_progress_bar();
//     plsync::sync(&src_path, &dest_path, &options, &progress_bar);

//     let dest_exe = &dest_path.join("a_dir/foo.exe");
//     assert_not_executable(dest_exe);
//     Ok(())
// }

#[test]
fn rewrite_partially_written_files() -> Result<(), std::io::Error> {
    let tmp_dir = TempDir::new()?;
    let (src_path, dest_path) = setup_test(tmp_dir.path());
    let src_top = src_path.join("top.txt");
    let expected = fs::read_to_string(src_top)?;
    let options = new_sync_options();
    let progress_bar = new_progress_bar();

    plsync::sync(&src_path, &dest_path, &options, &progress_bar);
    let dest_top = dest_path.join("top.txt");
    // Corrupt the dest/top.txt
    fs::write(&dest_top, "this is")?;

    plsync::sync(&src_path, &dest_path, &options, &progress_bar);
    let actual = fs::read_to_string(&dest_top)?;
    assert_eq!(actual, expected);
    Ok(())
}

#[test]
fn dest_read_only() -> Result<(), std::io::Error> {
    let tmp_dir = TempDir::new()?;
    let (src_path, dest_path) = setup_test(tmp_dir.path());
    fs::create_dir_all(&dest_path)?;

    let dest_top = dest_path.join("top.txt");
    fs::write(&dest_top, "this is read only")?;

    let mut perms = fs::metadata(&dest_top)?.permissions();
    perms.set_readonly(true);
    fs::set_permissions(&dest_top, perms)?;

    let src_top = src_path.join("top.txt");
    make_recent(&src_top)?;

    let options = new_sync_options();
    let progress_bar = new_progress_bar();
    let result = plsync::sync(&src_path, &dest_path, &options, &progress_bar);
    assert_eq!(result.errors_total(), 1);
    Ok(())
}

#[test]
#[cfg(unix)]
fn broken_link_in_src() -> Result<(), std::io::Error> {
    let tmp_dir = TempDir::new()?;
    let (src_path, dest_path) = setup_test(tmp_dir.path());
    let src_broken_link = &src_path.join("broken");
    unix::fs::symlink("no-such", src_broken_link)?;

    let options = new_sync_options();
    let progress_bar = new_progress_bar();
    let result = plsync::sync(&src_path, &dest_path, &options, &progress_bar);

    let dest_broken_link = &dest_path.join("broken");
    assert!(!dest_broken_link.exists());
    assert_eq!(dest_broken_link.read_link()?.to_string_lossy(), "no-such");
    assert!(result.errors_total() == 0);
    Ok(())
}

#[test]
#[cfg(unix)]
fn dry_run() -> Result<(), std::io::Error> {
    let tmp_dir = TempDir::new()?;
    let (src_path, dest_path) = setup_test(tmp_dir.path());
    let mut options = new_sync_options();
    options.perform_dry_run = true;
    let progress_bar = new_progress_bar();
    let outcome = plsync::sync(&src_path, &dest_path, &options, &progress_bar);

    assert!(outcome.errors_total() == 0);
    assert!(!dest_path.exists(), "{:?} does exist", dest_path);
    Ok(())
}
