# plsync

Parallel local only `rsync` implementation in Rust.

# Usage

```
$ cargo install plsync
$ plsync --progress test/src test/dest
[00:00:02] Enumerating 16.56 kentries at 6.97 kentries/sec [Copy phase]
```

# Caveat

We do everything we can to make sure data loss is impossible, but despite our best efforts, it may still happen.

Please make sure your files files are backed up if necessary before using `plsync` on sensitive data.

Thank you for your understanding!

# Features

* Easy to remember command line syntax.

* Print progress on one line, and erase it when done, thus avoiding flooding your terminal
  with useless noise.

* Unsurprising behavior: missing directories are created
  on the fly, files are only copied if:

  * destination is missing
  * destination exists but is older than the source
  * or source and destination have different sizes

# Command line options

* `--no-perms` do not preserve permissions (no-op on Windows)
* `--dry-run` perform a trial run with no changes made
* `--progress` show progress during transfer
* `--stats` give some file-transfer stats
* `--delete` delete extraneous files from dest dirs (not yet implemented)
* `--delete-before` receiver deletes before xfer, not during
* `--delete-after` receiver deletes after transfer, not during
* `--parallelism` maximum number of sync jobs (0 for number of online processors) [default: 0]
* `--log-level` set the log level [default: error] [possible values: error, warn, info, debug, trace]
* `--exclude` exclude files matching EXCLUDE
* `--help` print help
* `--version` print version
