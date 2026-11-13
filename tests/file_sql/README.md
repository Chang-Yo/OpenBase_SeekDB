# File SQL regression suite

Requires a built seekdb server and Python 3 with PyMySQL. Tests do not start or stop the server.
Copy `data/` into a fresh server-readable directory under `secure_file_priv`, then run:

```sh
python3 tests/file_sql/run.py --port 10020 --data /absolute/fresh/fixture-directory --output results.json
```

The fixture directory must be fresh: COPY tests intentionally reject overwriting existing files.
Expected rows and errors are declared in `cases.json`. The runner rejects connection failures as expected SQL errors.
The upstream `tools/deploy/mysql_test/test_suite/file_sql/` suite is unchanged.
Rust Parquet tests run with `cargo test --manifest-path rust/Cargo.toml -p file-sql`.
