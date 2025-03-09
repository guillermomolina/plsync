use std::path::{Path, PathBuf};
use filesize::PathExt;
use std::os::unix::fs::MetadataExt;

use jwalk::rayon::iter::{ParallelBridge, ParallelIterator};

// fn burrow(dir: &Path) -> Vec<PathBuf> {
//     std::fs::read_dir(dir)
//         .unwrap()
//         .par_bridge()
//         .flat_map(|entry| {
//             let entry = entry.unwrap().path();
//             if entry.is_dir() {
//                 burrow(&entry)
//             } else {
//                 vec![entry]
//             }
//         })
//         .collect()
// }

fn disk_usage(dir: &Path) ->u64 {
    std::fs::read_dir(dir)
        .unwrap()
        .par_bridge()
        .map(|entry| {
            let entry = entry.unwrap();
            let path = entry.path();
            let metadata = entry.metadata().unwrap();
            if metadata.is_dir() {
                disk_usage(&path)
            } else if metadata.nlink() > 1 {
                0
            } else {
                metadata.len()
                // path.size_on_disk_fast(&metadata).unwrap_or_else(|_| {
                //     0
                // })
            }
        })
        .sum()
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let source = PathBuf::from(&args[1]);
    let size = disk_usage(&source);
    println!("Total disk usage: {} bytes",  humansize::format_size(size, humansize::DECIMAL));
}