use clap::Parser;
use indicatif::{ProgressBar, ProgressDrawTarget, ProgressState, ProgressStyle};
use log::{error, info, warn};
use plsync::{sync, DecimalCount, SyncOptions};
use rayon::{ThreadPoolBuildError, ThreadPoolBuilder};
use std::env;
use std::fmt::Write;
use std::path::PathBuf;
use std::process;

const MAX_PARALLELISM: usize = 64;

#[derive(Debug, Parser)]
#[clap(name = "plsync", version = "0.1.0")]
struct Parameters {
    #[clap(
        long = "no-perms",
        help = "Do not preserve permissions (no-op on Windows)"
    )]
    no_preserve_permissions: bool,

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

    #[clap(long = "delete", help = "Delete extraneous files from dest dirs")]
    delete: bool,

    #[clap(
        short = 'p',
        long = "parallelism",
        help = "Maximum number of sync jobs (0 for number of online processors)",
        default_value = "0"
    )]
    parallelism: Option<usize>,

    #[clap(
        long = "log-level",
        help = "Set the log level",
        default_value = "error",
        value_parser = clap::builder::PossibleValuesParser::new(["error", "warn", "info", "debug", "trace"]),
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
            error!(
                "Requested parallelism is greater than {} processors.",
                MAX_PARALLELISM
            );
            process::exit(1);
        }
        parallelism
    }
}

fn set_thread_pool(num_threads: usize) -> Result<(), ThreadPoolBuildError> {
    if num_threads == 0 {
        ThreadPoolBuilder::new().build_global()
    } else {
        ThreadPoolBuilder::new()
            .num_threads(num_threads)
            .build_global()
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

    set_thread_pool(get_parallelism(&arguments)).expect("fields we set cannot fail");

    let options = SyncOptions {
        preserve_permissions: !arguments.no_preserve_permissions,
        perform_dry_run: arguments.perform_trial_run,
        delete: arguments.delete,
    };

    let progress_bar = ProgressBar::new(0);
    progress_bar.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] Synced {pos} entries at {per_sec_round}")
            .unwrap()
            .with_key(
                "per_sec_round",
                |state: &ProgressState, w: &mut dyn Write| {
                    write!(w, "{}/s", DecimalCount(state.per_sec())).unwrap()
                },
            ),
    );
    if !arguments.show_progress {
        progress_bar.set_draw_target(ProgressDrawTarget::hidden())
    }
    let sync_status = sync(source, destination, &options, &progress_bar);
    progress_bar.finish_and_clear();
    
    let errors_total = sync_status.errors_total();
    if arguments.show_stats {
        sync_status.print();
    }
    if errors_total > 0 {
        process::exit(1);
    } else {
        process::exit(0);
    }
}
