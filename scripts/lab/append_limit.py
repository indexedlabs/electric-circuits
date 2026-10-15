#!/usr/bin/env python3
"""Opt-in, real-storage regression for the large shape-append refusal.

This is deliberately a failing regression on an affected DS binary. No expected
failure decorator: a non-2xx append is a semantic failure, not a timeout pass.
The request is streamed so the probe does not allocate a transaction-sized body.
"""
import argparse
import http.client
import json
from pathlib import Path
import threading
import time

from hardening import Lab, http as request, port


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--engine', type=Path, required=True, help='identity only; engine is not started')
    parser.add_argument('--ds', type=Path, default=Path('/Users/mh/.cargo/bin/durable-streams-server'))
    parser.add_argument('--mb', type=int, default=500)
    args = parser.parse_args()
    if not 1 <= args.mb <= 500:
        parser.error('1..500 decimal MB only')
    args.engine = args.engine.resolve()
    args.ds = args.ds.resolve()
    args.scenario, args.shapes = 'append-limit', 0
    lab = Lab(args)
    try:
        lab.guard_thread = threading.Thread(target=lab.guard, daemon=True)
        lab.guard_thread.start()
        dsport = port()
        url = f'http://127.0.0.1:{dsport}/probe'
        lab.spawn('ds', [str(args.ds), '--host', '127.0.0.1', '--port', str(dsport),
                        '--data-dir', str(lab.data/'ds'), '--durability', 'wal'])
        lab.await_('ds-listening', lambda: request(f'http://127.0.0.1:{dsport}/')[0] in (200,404))
        status, _, body = request(url, 'PUT')
        assert status in (200,201,204), (status, body)
        row = json.dumps({'key':'1', 'value': {'id':1, 'payload':'x'*65536},
                          'headers': {'operation':'insert'}}).encode()
        rows = args.mb*1_000_000 // 65536
        size = 2 + rows*len(row) + rows-1
        connection = http.client.HTTPConnection('127.0.0.1', dsport, timeout=90)
        start = time.monotonic()
        try:
            connection.putrequest('POST', '/probe')
            connection.putheader('Content-Type', 'application/json')
            connection.putheader('Content-Length', str(size))
            connection.endheaders()
            connection.send(b'[')
            for i in range(rows):
                lab.check()
                connection.send((b',' if i else b'')+row)
            connection.send(b']')
            response = connection.getresponse()
            body = response.read().decode(errors='replace')
            lab.result.update(request_bytes=size, status=response.status, response=body,
                              append_ms=(time.monotonic()-start)*1000)
            assert response.status in (200,204), f'valid shape-sized append must land; HTTP {response.status}: {body}'
        finally:
            connection.close()
        lab.result['outcome'] = 'pass'
    except BaseException as error:
        lab.result.update(outcome='fail', error=repr(error))
        raise
    finally:
        lab.cleanup()


if __name__ == '__main__':
    main()
