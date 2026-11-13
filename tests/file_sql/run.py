#!/usr/bin/env python3
"""Run predeclared file SQL cases; preserve actual values and errors for audit."""
import argparse
import decimal
import json
from pathlib import Path
import time
import pymysql

parser = argparse.ArgumentParser()
parser.add_argument('--port', type=int, required=True)
parser.add_argument('--data', required=True)
parser.add_argument('--database', default='test')
parser.add_argument('--output', required=True)
args = parser.parse_args()
cases = json.loads(Path(__file__).with_name('cases.json').read_text())
results = []
def same(actual, expected):
    if isinstance(expected, list):
        return isinstance(actual, (list, tuple)) and len(actual) == len(expected) and all(same(a, e) for a, e in zip(actual, expected))
    if expected is None or isinstance(expected, str):
        return actual == expected
    if isinstance(expected, (int, float)):
        try:
            return decimal.Decimal(str(actual)) == decimal.Decimal(str(expected))
        except decimal.InvalidOperation:
            return False
    return actual == expected
connection = pymysql.connect(host='127.0.0.1', port=args.port, user='root', database=args.database, charset='utf8mb4', autocommit=True, read_timeout=90)
try:
    for case in cases:
        sql = case['sql'].replace('{data}', args.data.replace("'", "''"))
        result = {'id': case['id'], 'sql': sql, 'expected': case}
        started = time.monotonic()
        try:
            with connection.cursor() as cursor:
                cursor.execute(sql)
                rows = cursor.fetchall()
                columns = [item[0] for item in cursor.description] if cursor.description else []
            result.update(rows=rows, columns=columns)
            result['passed'] = not case.get('expect_error') and 'error' not in case and same(rows, case.get('rows', []))
            if 'columns' in case:
                result['passed'] &= columns == case['columns']
        except pymysql.MySQLError as error:
            result['error'] = {'code': error.args[0], 'message': str(error)}
            code = error.args[0]
            transport_failure = not isinstance(code, int) or code == 0 or 2000 <= code < 3000
            result['passed'] = not transport_failure and (case.get('expect_error', False) or case.get('error') == code)
            if transport_failure:
                result['transport_failure'] = True
        result['seconds'] = time.monotonic() - started
        results.append(result)
        Path(args.output).write_text(json.dumps(results, ensure_ascii=False, indent=2, default=str) + '\n')
        print(case['id'], 'PASS' if result['passed'] else 'FAIL', flush=True)
        if result.get('transport_failure'):
            print('Stopping: connection lost; remaining cases were not executed.', flush=True)
            break
finally:
    connection.close()
raise SystemExit(0 if len(results) == len(cases) and all(result['passed'] for result in results) else 1)
