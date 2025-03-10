use std::path::{Path, PathBuf};

use rayon::iter::{ParallelBridge, ParallelIterator};

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

struct Status {
    size: u64,
    dirs: u64,
    files: u64,
}

fn disk_usage(dir: &Path) -> Status {
    std::fs::read_dir(dir)
        .unwrap()
        .par_bridge()
        .map(|entry| {
            let entry = entry.unwrap();
            let path = entry.path();
            let metadata = entry.metadata().unwrap();
            if metadata.is_dir() {
                let mut status = disk_usage(&path);
                status.dirs += 1;
                status
            // } else if metadata.nlink() > 1 {
            //     0
            } else {
                let status = Status {
                    size: metadata.len(),
                    dirs: 0,
                    files: 1,
                };
                status
            }
        })
        .reduce(
            || Status {
                size: 0,
                dirs: 0,
                files: 0,
            },
            |a, b| Status {
                size: a.size + b.size,
                dirs: a.dirs + b.dirs,
                files: a.files + b.files,
            },
        )
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let source = PathBuf::from(&args[1]);
    let status = disk_usage(&source);
    println!(
        "Total dirs: {}, files: {}, disk usage: {}",
        status.dirs,
        status.files,
        humansize::format_size(status.size, humansize::BINARY)
    );
}
