# Third-Party Notices

## splunk-ddss-extractor / splunker journal decoder

The files in `frosty/splunk_journal/` are adapted from:

- **splunk-ddss-extractor** — MIT License — https://github.com/ponquersohn/splunk_ddss_extractor
- **splunker** — Apache License 2.0 — https://github.com/fionera/splunker

The Splunk journal binary format was originally reverse-engineered and implemented in Go by fionera/splunker. splunk-ddss-extractor ported that logic to pure Python (and optionally Rust). Frosty vendors the pure-Python decoder to avoid external build dependencies.
