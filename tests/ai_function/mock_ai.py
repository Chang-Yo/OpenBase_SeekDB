#!/usr/bin/env python3
"""Local Lab1 protocol fixture, based on course DEVELOPMENT.txt appendix A.
Never a substitute for official grading. No real API keys are logged.
Run with --log pointing to a fresh experiment directory.
"""
import argparse
import hashlib
import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlsplit


def vector(text, dimension):
    # Stable across processes, distinguishes different texts of the same length.
    base = int.from_bytes(hashlib.sha256(text.encode()).digest()[:4], 'big') % 10000
    return [(base + i) / 10000 for i in range(dimension)]


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass

    def send(self, status, payload):
        data = payload if isinstance(payload, bytes) else json.dumps(payload, ensure_ascii=False).encode()
        self.send_response(status)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.send_header('Content-Length', str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        self.send(200 if self.path == '/health' else 404, {'fixture': 'lab1-local-v1'})

    def do_POST(self):
        raw = self.rfile.read(int(self.headers.get('Content-Length', '0')))
        try:
            req = json.loads(raw)
        except ValueError:
            return self.send(400, {'error': 'invalid request JSON'})
        with self.server.log_lock:
            with self.server.log_path.open('a', encoding='utf8') as log:
                log.write(json.dumps({'time_ns': time.time_ns(), 'path': self.path, 'request': req}, ensure_ascii=False) + '\n')
        path = urlsplit(self.path).path.replace('/compatible-mode', '', 1)
        mode = path.rsplit('/', 1)[-1]
        if mode == 'retry_once':
            with self.server.log_lock:
                attempts = self.server.attempts.get(self.path, 0)
                self.server.attempts[self.path] = attempts + 1
            if attempts == 0:
                return self.send(503, {'error': 'transient fixture failure'})
        if mode == 'slow':
            time.sleep(3)
        if mode == 'http500':
            return self.send(500, {'error': 'fixture failure'})
        if mode == 'malformed':
            return self.send(200, b'{"choices":[')
        if mode == 'root_array':
            return self.send(200, [])
        if mode == 'root_null':
            return self.send(200, None)
        try:
            if path.startswith('/v1/chat/completions') or path.startswith('/api/generate'):
                ollama = path.startswith('/api/generate')
                if ollama and req.get('stream') is not False:
                    return self.send(400, {'error': 'stream must be false'})
                prompt = req['prompt'] if ollama else req['messages'][-1]['content']
                content = 'echo:' + prompt
                alternatives = {'empty': '', 'null': None, 'number': 123,
                                'object': {'answer': '中文"\\\n'}, 'array': ['中文', 2]}
                content = alternatives.get(mode, content)
                payload = {'model': req['model'], 'done': True, 'response': content} if ollama else {
                    'choices': [{'index': 0, 'message': {'role': 'assistant', 'content': content}}]}
                if not ollama:
                    if mode == 'missing': payload = {}
                    if mode == 'choices_type': payload['choices'] = {}
                    if mode == 'choices_empty': payload['choices'] = []
                    if mode == 'message_type': payload['choices'][0]['message'] = []
                return self.send(200, payload)
            if path.startswith('/v1/embeddings') or path.startswith('/api/embed'):
                texts = req['input']
                if not isinstance(texts, list) or not texts or any(not isinstance(t, str) for t in texts):
                    return self.send(400, {'error': 'input must be a nonempty string array'})
                dim = req.get('dimensions', self.server.embedding_dimension)
                if type(dim) is not int or not 0 < dim <= 65536:
                    return self.send(400, {'error': 'invalid dimensions'})
                data = [{'index': i, 'embedding': vector(t, dim)} for i, t in enumerate(texts)]
                if mode == 'shuffle': data.reverse()
                if mode == 'duplicate': data.append(data[0].copy())
                if mode == 'missing': data.pop()
                if mode == 'index_range': data[0]['index'] = len(texts)
                if mode == 'index_type': data[0]['index'] = '0'
                if mode == 'wrong_dimension': data[0]['embedding'].append(1.0)
                if mode == 'bad_element': data[0]['embedding'][0] = 'bad'
                if mode == 'null_vector': data[0]['embedding'] = None
                if path.startswith('/api/embed'):
                    return self.send(200, {'embeddings': [d['embedding'] for d in data]})
                return self.send(200, {'data': data})
            if path.startswith('/dashscope/embeddings'):
                texts = req['input']['texts']
                dim = req.get('parameters', {}).get('dimension', 3)
                data = [{'text_index': i, 'embedding': vector(t, dim)} for i, t in enumerate(texts)]
                if mode == 'shuffle': data.reverse()
                if mode == 'duplicate' and len(data) > 1: data[1]['text_index'] = data[0]['text_index']
                if mode == 'output_type': return self.send(200, {'output': []})
                return self.send(200, {'output': {'embeddings': data}})
            if path.startswith('/dashscope/generate'):
                if mode == 'output_type': return self.send(200, {'output': []})
                return self.send(200, {'output': {'choices': [{'message': {
                    'content': 'echo:' + req['input']['messages'][-1]['content']}}]}})
            if path.startswith('/dashscope/rerank'):
                if mode == 'output_type': return self.send(200, {'output': []})
                data = [{'index': i, 'relevance_score': float(len(t))}
                        for i, t in enumerate(req['input']['documents'])]
                if mode == 'shuffle': data.reverse()
                return self.send(200, {'output': {'results': data}})
            if path.startswith('/v1/rerank'):
                query_tokens = set(req['query'].casefold().split())
                data = [{'index': i, 'relevance_score': float(len(t) if self.server.rerank_scoring == 'length'
                    else len(query_tokens & set(t.casefold().split())))} for i, t in enumerate(req['documents'])]
                if mode == 'shuffle': data.reverse()
                if mode == 'duplicate': data.append(data[0].copy())
                if mode == 'missing': data.pop()
                if mode == 'index_range': data[0]['index'] = len(req['documents'])
                if mode == 'bad_score': data[0]['relevance_score'] = 'bad'
                return self.send(200, {'results': data})
        except (KeyError, TypeError, IndexError) as exc:
            return self.send(400, {'error': 'request schema error: ' + type(exc).__name__})
        self.send(404, {'error': 'unknown fixture endpoint'})


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', type=int, default=18080)
    parser.add_argument('--log', type=Path, required=True)
    parser.add_argument('--embedding-dimension', type=int, default=3)
    parser.add_argument('--rerank-scoring', choices=['length', 'token-overlap'], default='length')
    args = parser.parse_args()
    args.log.parent.mkdir(parents=True, exist_ok=True)
    server = ThreadingHTTPServer(('127.0.0.1', args.port), Handler)
    server.embedding_dimension = args.embedding_dimension
    server.rerank_scoring = args.rerank_scoring
    server.log_path = args.log
    server.log_lock = threading.Lock()
    server.attempts = {}
    print(f'fixture port={args.port} requests={args.log}', flush=True)
    server.serve_forever()
