# AI function protocol regression

Use a freshly deployed seekdb instance and an otherwise idle local fixture. The cases create `regression_` models/endpoints and tables; run once per fresh instance. Expected rows, SQL errors and HTTP request counts are declared in `cases.json` before execution.

From the repository root, start the fixture in a separate terminal:

```sh
python3 tests/ai_function/mock_ai.py --port 18080 --log /tmp/lab1-requests.jsonl
```

Then run:

```sh
python3 tests/ai_function/run_sql_cases.py \
  --client deps/3rd/u01/obclient/bin/obclient --port 10010 \
  --source "$PWD" --binary build_release/src/observer/seekdb \
  --cases tests/ai_function/cases.json \
  --requests /tmp/lab1-requests.jsonl --output /tmp/lab1-results
```

The output directory must not already exist. Verify the running server executable matches `--binary`. When source has changed since compilation, pass `--source-patch` with the saved patch used to build that binary. The runner records cases, source/build patches, binary hash, SQL output, errors and per-case request logs. `MYSQL_PWD` may supply a local test password; it is not written to the manifest.

The original mysqltest suite remains under `tools/deploy/mysql_test/test_suite/ai_function`. Its integration test uses port 8000. A local fixture compatible with its example expectations can be started with `--port 8000 --embedding-dimension 768 --rerank-scoring token-overlap`. This is a local protocol fixture, not the organizer grading service. Do not change original expected results to match a fixture.

Endpoint `max_concurrency` metadata adds a system-table column. Use a fresh data directory for this competition build. The field is validated and persisted; this patch does not add a global concurrent-request scheduler. Embedding batches contain at most 20 inputs and run sequentially within one expression evaluation.
