# plsync

Parallel local only `rsync` implementation in Rust.

# Usage

```
$ cargo install plsync
$ plsync test/src test/dest
:: Syncing from test/src to test/dest …
 50% 24/50 Downloads/archlinux.iso   00:01:30
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

