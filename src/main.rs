use clap::Parser;
use log::{error, info, warn};
use plsync::{sync, SyncOptions};
use std::env;
use std::path::PathBuf;
use std::process;

const MAX_PARALLELISM: usize = 64;

#[derive(Debug, Parser)]
#[clap(name = "plsync")]
struct Parameters {
    #[clap(
        long = "no-perms",
        help = "Do not preserve permissions (no-op on Windows)"
    )]
    no_preserve_permissions: bool,

    #[clap(long = "err-list", help = "Write errors to the given file")]
    error_list_path: Option<PathBuf>,

    #[clap(
        short = 'n',
        long = "dry-run",
        help = "Perform a trial run with no changes made"
    )]
    perform_trial_run: bool,

    #[clap(long = "progress", help = "Show progress during transfer")]
    show_progress: bool,

    #[clap(long = "stats", help = "Give some file-transfer stats")]
    show_stats: bool,

    #[clap(
        short = 'p',
        long = "parallelism",
        help = "Allow up to n sync jobs (default is the number of online processors)",
    )]
    parallelism: Option<usize>,

    #[clap(
        long = "log-level", 
        help = "Set the log level (e.g., info, debug, trace)"
    )]
    log_level: Option<String>,

    #[clap(value_parser)]
    source: PathBuf,

    #[clap(value_parser)]
    destination: PathBuf,
}

fn get_parallelism(arguments: &Parameters) -> usize {
    let available_parallelism = std::thread::available_parallelism().unwrap().get();
    if arguments.parallelism.is_none() {
        available_parallelism
    } else {
        let parallelism = arguments.parallelism.unwrap();
        if parallelism > available_parallelism {
            warn!("Requested parallelism is greater than available processors.");
        }
        if parallelism > MAX_PARALLELISM {
            error!("Requested parallelism is greater than {} processors.", MAX_PARALLELISM);
            process::exit(1);
        }
        parallelism
    }
}

fn main() {
    let arguments = Parameters::parse();

    // Set log level based on command-line argument or environment variable
    if let Some(log_level) = &arguments.log_level {
        env::set_var("RUST_LOG", log_level);
    }
    env_logger::init();

    info!("Starting plsync");
    let source = &arguments.source;
    if !source.is_dir() {
        eprintln!("{} is not a directory", source.to_string_lossy());
        process::exit(1);
    }
    let destination = &arguments.destination;

    let options = SyncOptions {
        preserve_permissions: !arguments.no_preserve_permissions,
        perform_dry_run: arguments.perform_trial_run,
        parallelism: get_parallelism(&arguments),
        show_progress: arguments.show_progress,
        show_stats: arguments.show_stats,
    };

    let stats = sync(source, destination, options);
    match stats {
        Err(err) => {
            eprintln!("{}", err);
            process::exit(1);
        }
        Ok(errors) if errors > 0 => {
            process::exit(1);
        }
        _ => {
            process::exit(0);
        }
    }
}
