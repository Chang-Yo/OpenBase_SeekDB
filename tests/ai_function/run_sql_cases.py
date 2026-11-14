#!/usr/bin/env python3
"""Execute an explicitly prepared SQL case list and preserve per-case evidence.
Input JSON entries: name, sql, expected_errno OR expected_rows, optional request_count.
Uses obclient/mysql CLI; password may be supplied via MYSQL_PWD, never captured.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import time

parser = argparse.ArgumentParser()
parser.add_argument('--client', required=True)
parser.add_argument('--port', required=True, type=int)
parser.add_argument('--user', default='root')
parser.add_argument('--database', default='test')
parser.add_argument('--cases', required=True, type=Path)
parser.add_argument('--output', required=True, type=Path)
parser.add_argument('--requests', type=Path)
parser.add_argument('--source', required=True, type=Path)
parser.add_argument('--binary', required=True, type=Path)
parser.add_argument('--source-patch', type=Path, help='Saved build input patch when the working tree has changed since compilation')
args = parser.parse_args()
cases = json.loads(args.cases.read_text(encoding='utf-8-sig'))
for case in cases:
    if ('expected_errno' in case) == ('expected_rows' in case):
        raise ValueError('Each case must specify exactly one expected result')
args.output.mkdir(parents=True, exist_ok=False)
source_sha = subprocess.check_output(['git', '-C', str(args.source), 'rev-parse', 'HEAD'], text=True).strip()
patch = subprocess.check_output(['git', '-C', str(args.source), 'diff', '--binary'])
(args.output / 'source.patch').write_bytes(patch)
build_patch = args.source_patch.read_bytes() if args.source_patch else patch
(args.output / 'build-input.patch').write_bytes(build_patch)
command = [args.client, '-h127.0.0.1', '-P'+str(args.port), '-u'+args.user,
           '-D'+args.database, '--batch', '--raw', '--skip-column-names', '--default-character-set=utf8mb4']
manifest = {'started_ns': time.time_ns(), 'source_commit': source_sha,
            'patch_sha256': hashlib.sha256(patch).hexdigest(),
            'build_patch_sha256': hashlib.sha256(build_patch).hexdigest(),
            'build_patch_source': str(args.source_patch) if args.source_patch else 'current working tree',
            'binary_sha256': hashlib.file_digest(args.binary.open('rb'), 'sha256').hexdigest(),
            'command': command, 'cases': cases, 'cases_sha256': hashlib.sha256(args.cases.read_bytes()).hexdigest(),
            'request_log': str(args.requests) if args.requests else None,
            'scope': 'local SQL regression; does not certify official score'}
(args.output / 'manifest.json').write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding='utf8')
results = []
for index, case in enumerate(cases):
    before = args.requests.stat().st_size if args.requests and args.requests.exists() else 0
    started = time.time_ns()
    try:
        proc = subprocess.run(command, input=case['sql'], text=True, encoding='utf8', capture_output=True, timeout=180)
        stdout, stderr, code = proc.stdout, proc.stderr, proc.returncode
    except subprocess.TimeoutExpired as exc:
        stdout = exc.stdout.decode('utf8', 'replace') if isinstance(exc.stdout, bytes) else (exc.stdout or '')
        stderr = 'SQL case timed out'; code = -1
    prefix = args.output / f'{index:03d}'
    prefix.with_suffix('.sql').write_text(case['sql'], encoding='utf8')
    prefix.with_suffix('.stdout').write_text(stdout, encoding='utf8')
    prefix.with_suffix('.stderr').write_text(stderr, encoding='utf8')
    match = re.search(r'ERROR\s+(\d+)', stderr)
    errno = int(match[1]) if match else None
    passed = (code != 0 and errno == case['expected_errno']) if 'expected_errno' in case else (
        code == 0 and stdout.splitlines() == case['expected_rows'])
    requests = None
    if args.requests:
        raw = b''
        if args.requests.exists():
            with args.requests.open('rb') as log:
                log.seek(before); raw = log.read()
        prefix.with_suffix('.requests.jsonl').write_bytes(raw)
        requests = len(raw.splitlines())
    if 'request_count' in case:
        passed = passed and requests == case['request_count']
    result = {'name': case.get('name', str(index)), 'exit': code, 'errno': errno,
              'passed': passed, 'request_count': requests, 'elapsed_ns': time.time_ns()-started}
    results.append(result)
    (args.output / 'results.json').write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding='utf8')
print(json.dumps({'passed': sum(x['passed'] for x in results), 'total': len(results)}))
raise SystemExit(0 if all(x['passed'] for x in results) else 1)
