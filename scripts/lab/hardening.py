#!/usr/bin/env python3
"""Local, destructive-to-its-own-fixture hardening lab; Python standard library only.

Uses the conformance harness topology and native shape stream fold. No ambient service
configuration or credentials are inherited. Builds are separate; see README.md.
"""
import argparse
import hashlib
import http.client as http_client
import json
import math
import os
from pathlib import Path
import random
import shutil
import signal
import socket
import subprocess
import threading
import time
import urllib.parse
import uuid

ROOT = Path(__file__).resolve().parents[2]
PG = Path('/opt/homebrew/opt/postgresql@18/bin')
SCOPE = '/circuits/v1/test-stack/stores/ff8b5fa6-e786-4994-8da0-f14e9e79f318/queries/test-query/'
MAX_CREATED = 8_000_000_000
MIN_FREE = 12_000_000_000


def disk_violation(created, free):
    return created >= MAX_CREATED - 300_000_000 or free < MIN_FREE + 300_000_000


def http(url, method='GET', body=None, admin=False, timeout=10):
    u = urllib.parse.urlsplit(url)
    if u.scheme != 'http' or u.hostname != '127.0.0.1':
        raise ValueError('lab HTTP must be literal loopback')
    c = http_client.HTTPConnection(u.hostname, u.port, timeout=timeout)
    headers = {'Content-Type': 'application/json'}
    if admin:
        headers['Authorization'] = 'Bearer local-lab-only'
    try:
        c.request(method, u.path + ('?' + u.query if u.query else ''),
                  None if body is None else json.dumps(body), headers)
        r = c.getresponse()
        data = r.read()
        try:
            decoded = json.loads(data) if data else None
        except json.JSONDecodeError:
            decoded = data.decode(errors='replace')
        return r.status, {k.lower(): v for k, v in r.getheaders()}, decoded
    finally:
        c.close()


def port():
    with socket.socket() as s:
        s.bind(('127.0.0.1', 0))
        return s.getsockname()[1]


def percentile(values, q):
    return sorted(values)[max(0, math.ceil(q * len(values)) - 1)] if values else None


def summary(values):
    return {'count': len(values), 'p50_ms': percentile(values, .5),
            'p99_ms': percentile(values, .99), 'max_ms': max(values, default=None)}


def digest_row(row):
    row = dict(row)
    if 'payload' in row:
        data = row.pop('payload').encode()
        row['payload_md5'] = hashlib.md5(data).hexdigest()
        row['payload_bytes'] = len(data)
    return row


class SQL:
    """One owned psql connection, explicit completion fence, no .psqlrc/.pgpass."""
    def __init__(self, lab):
        self.lab = lab
        self.p = lab.spawn('psql', [str(PG / 'psql'), '-X', '-Atq', '-w',
                                   '-v', 'ON_ERROR_STOP=1', lab.pgurl], pipe=True)
        self.lock = threading.Lock()

    def run(self, sql):
        with self.lock:
            self.lab.check()
            fence = 'lab_' + uuid.uuid4().hex
            self.p.stdin.write(sql + '\n\\echo ' + fence + '\n')
            self.p.stdin.flush()
            rows = []
            while True:
                line = self.p.stdout.readline()
                if not line:
                    raise RuntimeError(f'psql exited during {sql[:120]}; see psql log')
                if line.rstrip() == fence:
                    return rows
                rows.append(line.rstrip())

    def close(self):
        if self.p.poll() is None:
            self.p.stdin.close()
            self.p.wait(timeout=10)


class Feed:
    def __init__(self, lab, rec):
        self.lab, self.rec = lab, rec
        self.rows, self.offset = {}, '-1'
        self.lock = threading.Lock()
        self.errors = []
        self.stop = threading.Event()
        self.thread = threading.Thread(target=self.loop, daemon=True)
        self.thread.start()

    def read(self, live=False):
        with self.lock:
            suffix = '?offset=' + urllib.parse.quote(self.offset)
            if live:
                suffix += '&live=long-poll'
            status, headers, events = http(self.rec['streamUrl'] + suffix, timeout=3)
            if status not in (200, 204):
                raise RuntimeError(f'feed {self.rec["shapeId"]}: HTTP {status}: {events}')
            for ev in events or []:
                if ev.get('headers', {}).get('operation') == 'delete':
                    self.rows.pop(ev['key'], None)
                elif ev.get('value') is not None:
                    row = digest_row(ev['value'])
                    self.rows[ev['key']] = row
                    if self.rec['table'] == 'ticks':
                        self.lab.seen(row['n'], time.time())
            self.offset = headers.get('stream-next-offset', self.offset)
            return 'stream-up-to-date' in headers

    def loop(self):
        while not self.stop.is_set() and not self.lab.stop.is_set():
            try:
                self.read(live=True)
            except Exception as e:
                self.errors.append(str(e))
                self.stop.wait(.05)

    def close(self):
        self.stop.set()
        self.thread.join(timeout=5)
        if self.thread.is_alive():
            raise RuntimeError('feed reader did not join')

    def verify(self, sql):
        # Called only after writer quiescence and a server receipt. Stop the live
        # reader, then read through a fresh public tail and commit that fold.
        self.close()
        for _ in range(10000):
            if self.read():
                break
        else:
            raise AssertionError('public stream did not reach tail')
        if self.rec['table'] == 'ticks':
            query = f"SELECT row_to_json(t) FROM (SELECT * FROM ticks WHERE cohort={self.rec['cohort']} ORDER BY id) t"
        else:
            query = "SELECT row_to_json(t) FROM (SELECT id,n,md5(payload) AS payload_md5,octet_length(payload) AS payload_bytes FROM bulk ORDER BY id) t"
        oracle = {str(r['id']): r for r in map(json.loads, sql.run(query + ';'))}
        if self.rows != oracle:
            missing = sorted(oracle.keys() - self.rows.keys())[:10]
            extra = sorted(self.rows.keys() - oracle.keys())[:10]
            bad = [k for k in self.rows.keys() & oracle.keys() if self.rows[k] != oracle[k]][:10]
            raise AssertionError(f'client/SQL mismatch missing={missing} extra={extra} values={bad}')
        return len(oracle)


class Lab:
    def __init__(self, args):
        self.args = args
        self.name = args.scenario + '-' + time.strftime('%Y%m%d-%H%M%S')
        self.out = ROOT / '.agent/results' / self.name
        self.data = ROOT / '.agent/lab-data' / self.name
        self.out.mkdir(parents=True)
        self.data.mkdir(parents=True)
        self.stop = threading.Event()
        self.children, self.logs, self.feeds = [], [], []
        self.samples, self.commits, self.arrivals, self.receipts = [], {}, {}, {}
        self.errors, self.result = [], {'scenario': args.scenario, 'shapes': args.shapes}
        self.result.update(runner_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            engine_sha256=hashlib.sha256(args.engine.read_bytes()).hexdigest(),
            ds_sha256=hashlib.sha256(args.ds.read_bytes()).hexdigest(),
            git_head=subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip())
        self.engine = None
        self.engineurl = None
        self.sequence = 0
        self.guard_error = None
        self.guard_thread = None
        self.adapter_stop = threading.Event()
        self.adapter_thread = None
        self.adapter_observations = []
        self.env = {'PATH': '/opt/homebrew/bin:/usr/bin:/bin', 'HOME': str(self.data),
                    'TMPDIR': str(self.data), 'LC_ALL': 'C', 'PGPASSFILE': str(self.data/'absent-passfile')}
        self.sql = None
        self.write_stop = threading.Event()
        self.writer = None

    def record(self, kind, **data):
        with (self.out/'journal.jsonl').open('a') as f:
            f.write(json.dumps({'at': time.time(), 'kind': kind, **data}) + '\n')

    def spawn(self, name, argv, env=None, pipe=False):
        f = (self.out/f'{name}-{len(self.children)}.log').open('w')
        self.logs.append(f)
        p = subprocess.Popen(argv, env=env or self.env, cwd=self.data,
                             stdin=subprocess.PIPE if pipe else subprocess.DEVNULL,
                             stdout=subprocess.PIPE if pipe else f, stderr=f,
                             text=True, start_new_session=True)
        self.children.append((name, p))
        self.record('spawn', name=name, pid=p.pid, argv=argv)
        return p

    def check(self):
        if self.guard_error:
            raise RuntimeError(self.guard_error)
        if self.stop.is_set():
            raise RuntimeError('lab stopped')

    def guard(self):
        while not self.stop.is_set():
            used = int(subprocess.run(['/usr/bin/du', '-sk', str(ROOT/'.agent')], capture_output=True).stdout.split()[0]) * 1024
            free = shutil.disk_usage('/').free
            rss = {}
            for name, p in list(self.children):
                if p.poll() is None and name.startswith(('engine', 'ds', 'postgres')):
                    value = subprocess.run(['/bin/ps', '-p', str(p.pid), '-o', 'rss='], capture_output=True, text=True).stdout.strip()
                    if value:
                        rss[str(p.pid)] = {'name': name, 'bytes': int(value) * 1024}
            self.samples.append({'at': time.time(), 'created_bytes': used, 'free_bytes': free, 'rss': rss})
            if disk_violation(used, free):
                self.guard_error = f'disk guard: created={used}, free={free}; stopping with 300 MB headroom'
                self.record('disk_stop', error=self.guard_error)
                self.stop.set()
                for _, p in list(self.children):
                    if p.poll() is None:
                        os.killpg(p.pid, signal.SIGKILL)
                return
            self.stop.wait(1)

    def await_(self, name, predicate, seconds=120):
        end = time.monotonic() + seconds
        last = None
        while time.monotonic() < end:
            self.check()
            try:
                value = predicate()
                if value:
                    self.record('gate', name=name)
                    return value
            except (OSError, http_client.HTTPException) as e:
                last = str(e)
            self.stop.wait(.01)
        raise TimeoutError(f'{name}: deadline {seconds}s; last error={last}')

    def setup(self, managed=False):
        # Proactive headroom; no scenario starts if even its fixed reserve is absent.
        free = shutil.disk_usage('/').free
        if free < MIN_FREE + 2_000_000_000:
            raise RuntimeError(f'insufficient initial disk headroom: {free}')
        self.guard_thread = threading.Thread(target=self.guard, daemon=True)
        self.guard_thread.start()
        p = self.spawn('initdb', [str(PG/'initdb'), '-D', str(self.data/'pg'), '-U', 'postgres', '--auth=trust', '--no-sync'])
        if p.wait(timeout=30):
            raise RuntimeError('initdb failed')
        pgport = port()
        self.pgurl = f'postgres://postgres@127.0.0.1:{pgport}/postgres?sslmode=disable'
        self.spawn('postgres', [str(PG/'postgres'), '-D', str(self.data/'pg'), '-h', '127.0.0.1', '-p', str(pgport),
                   '-c', 'unix_socket_directories=', '-c', 'wal_level=logical',
                   '-c', 'max_replication_slots=8', '-c', 'max_wal_senders=8',
                   '-c', 'max_wal_size=256MB', '-c', 'min_wal_size=80MB',
                   '-c', 'logical_decoding_work_mem=32MB'])
        def pg_up():
            with socket.create_connection(('127.0.0.1', pgport), timeout=.2):
                return True
        self.await_('pg-listening', pg_up)
        self.sql = SQL(self)
        self.sql.run('''CREATE TABLE ticks (id int PRIMARY KEY, cohort int NOT NULL, n int NOT NULL);
CREATE TABLE bulk (id int PRIMARY KEY, n int NOT NULL, payload text NOT NULL);
ALTER TABLE bulk ALTER COLUMN payload SET STORAGE EXTERNAL;
CREATE TABLE untracked (id int PRIMARY KEY, n int NOT NULL, payload text NOT NULL);
ALTER TABLE untracked ALTER COLUMN payload SET STORAGE EXTERNAL;
CREATE TABLE circuits_source_fence (source_commit_id uuid PRIMARY KEY);
ALTER TABLE circuits_source_fence REPLICA IDENTITY FULL;
CREATE TABLE __el_sync (id int PRIMARY KEY, n bigint NOT NULL);
INSERT INTO __el_sync VALUES (1,0);
CREATE SCHEMA electric_circuits;
CREATE TABLE electric_circuits.writer_ownership (
 coordination_key char(64) PRIMARY KEY, generation bigint NOT NULL CHECK (generation>=1),
 owner_revision text NOT NULL, phase text NOT NULL CHECK (phase IN ('active','quiesced')),
 handoff_id uuid, source_commit_id uuid, updated_at timestamptz NOT NULL DEFAULT statement_timestamp(),
 CHECK (phase='active' OR (handoff_id IS NOT NULL AND source_commit_id IS NOT NULL)));
''')
        dsport = port()
        self.dsurl = f'http://127.0.0.1:{dsport}'
        self.spawn('ds', [str(self.args.ds), '--host', '127.0.0.1', '--port', str(dsport),
                         '--data-dir', str(self.data/'ds'), '--durability', 'wal', '--long-poll-timeout-ms', '500'])
        self.await_('ds-listening', lambda: http(self.dsurl+'/')[0] in (200, 404))
        self.revision = str(uuid.uuid4()) if managed else None
        self.engine, self.engineurl, boot = self.start_engine('A', self.revision, initial=managed)
        self.result['initial_boot_ms'] = boot
        for i in range(self.args.shapes):
            rec = self.request('/shapes', 'POST', {'table': 'ticks', 'where': {'col': 'cohort', 'op': 'eq', 'value': i}})
            rec.update(table='ticks', cohort=i)
            self.feeds.append(Feed(self, rec))
        rec = self.request('/shapes', 'POST', {'table': 'bulk', 'where': {'col': 'n', 'op': 'gte', 'value': 0}})
        rec.update(table='bulk')
        self.feeds.append(Feed(self, rec))

    def request(self, path, method='GET', body=None, url=None):
        status, _, data = http((url or self.engineurl)+path, method, body, path.startswith('/_admin/'))
        if status not in (200, 201, 204):
            raise AssertionError(f'{method} {path}: {status}: {data}')
        return data

    def start_engine(self, label, revision=None, initial=False, ready=True):
        bind = port()
        env = dict(self.env)
        env.update(ELECTRIC_CIRCUITS_BIND=f'127.0.0.1:{bind}', ELECTRIC_CIRCUITS_DS_URL=self.dsurl,
                   ELECTRIC_CIRCUITS_DS_IN_PROCESS_TEST='1', ELECTRIC_CIRCUITS_INITIALIZE_NAMESPACE='1',
                   ELECTRIC_CIRCUITS_PG_URL=self.pgurl, ELECTRIC_CIRCUITS_PG_TABLES='ticks,bulk',
                   ELECTRIC_CIRCUITS_PG_SLOT='lab_slot', ELECTRIC_CIRCUITS_CONTROL_SECRET='local-lab-only',
                   ELECTRIC_CIRCUITS_LOG='info', ELECTRIC_CIRCUITS_DBSP_DIR=str(self.data/f'dbsp-{label}'),
                   ELECTRIC_CIRCUITS_TXN_SPILL_DIR=str(self.data/f'spill-{label}'),
                   ELECTRIC_CIRCUITS_SUBSCRIPTION_LEASE_SECS='3600', ELECTRIC_CIRCUITS_SHAPE_IDLE_SECS='3600')
        if revision:
            env['ELECTRIC_CIRCUITS_MANAGED_DEPLOYMENT_REVISION'] = revision
        if initial:
            env['ELECTRIC_CIRCUITS_MANAGED_DEPLOYMENT_INITIAL_ACTIVE'] = '1'
        self.record('engine-config', label=label, env=env)
        started = time.monotonic()
        p = self.spawn('engine-'+label, [str(self.args.engine)], env)
        url = f'http://127.0.0.1:{bind}'
        def listening():
            if p.poll() is not None:
                raise RuntimeError(f'engine {label} exited {p.returncode}')
            return http(url+('/ready' if ready else '/health'))[0] == 200
        self.await_('engine-'+label+('-ready' if ready else '-bound'), listening, 180)
        return p, url, (time.monotonic()-started)*1000

    def seen(self, n, at):
        self.arrivals.setdefault(n, at)

    def write_tick(self, sql):
        self.sequence += 1
        n = self.sequence
        marker = str(uuid.uuid4())
        sql.run(f"BEGIN; INSERT INTO ticks VALUES ({n},{n % self.args.shapes},{n}); INSERT INTO circuits_source_fence VALUES ('{marker}'); COMMIT;")
        at = time.time()
        self.commits[n] = (at, marker)
        self.record('commit', n=n, marker=marker, committed_at=at)
        return n, marker, at

    def start_load(self, count=100):
        def writer():
            c = SQL(self)
            try:
                for _ in range(count):
                    if self.write_stop.is_set() or self.stop.is_set():
                        break
                    start = time.monotonic()
                    self.write_tick(c)
                    self.write_stop.wait(max(0, 1/self.args.rate-(time.monotonic()-start)))
            except Exception as e:
                self.errors.append('writer: '+str(e))
            finally:
                c.close()
        self.writer = threading.Thread(target=writer, daemon=True)
        self.writer.start()

    def join_load(self):
        if self.writer:
            self.writer.join(timeout=120)
            if self.writer.is_alive():
                raise RuntimeError('writer did not finish')

    def watch_adapter(self):
        # Controller-selected loopback endpoint, not an ALB qualification. Keep
        # the Electric handle/offset across restarts and retain reset responses.
        def watch():
            offset, handle = '-1', None
            while not self.adapter_stop.is_set() and not self.stop.is_set():
                query = {'table': 'ticks', 'where': 'cohort = 0', 'offset': offset}
                if handle:
                    query['handle'] = handle
                try:
                    status, headers, body = http(self.engineurl+'/v1/shape?'+urllib.parse.urlencode(query), timeout=2)
                    if status == 200:
                        handle = headers.get('electric-handle', handle)
                        offset = headers.get('electric-offset', offset)
                    elif status == 409:
                        offset, handle = '-1', None
                    self.adapter_observations.append({'at': time.time(), 'status': status,
                        'retry_after': headers.get('retry-after')})
                except (OSError, http_client.HTTPException) as e:
                    self.adapter_observations.append({'at': time.time(), 'status': 'transport', 'error': str(e)})
                self.adapter_stop.wait(.05)
        self.adapter_thread = threading.Thread(target=watch, daemon=True)
        self.adapter_thread.start()

    def drain(self, marker, url=None, seconds=240):
        return self.await_('drained-'+marker,
            lambda: self.request('/_admin/drained-through/'+marker, url=url).get('drained'), seconds)

    def final_compare(self):
        self.join_load()
        n, marker, at = self.write_tick(self.sql)
        self.drain(marker)
        self.result['final_receipt_ms'] = (time.time()-at)*1000
        self.result['verified_rows'] = [f.verify(self.sql) for f in self.feeds]
        self.result['feed_errors'] = [e for f in self.feeds for e in f.errors]
        if self.errors:
            raise AssertionError(self.errors)
        self.result['change_latency'] = summary([max(0, (self.arrivals[k]-v[0])*1000)
            for k, v in self.commits.items() if k in self.arrivals])
        arrivals = sorted(self.arrivals.values())
        self.result['max_native_progress_gap_ms'] = max(((b-a)*1000 for a,b in zip(arrivals, arrivals[1:])), default=0)
        missing = sorted(self.commits.keys()-self.arrivals.keys())
        if missing:
            raise AssertionError(f'unobserved committed ticks {missing[:10]}')

    def stop_engine(self, sig=signal.SIGTERM):
        start = time.monotonic()
        self.engine.send_signal(sig)
        code = self.engine.wait(timeout=50)
        self.record('engine-exit', pid=self.engine.pid, signal=int(sig), code=code)
        self.result.setdefault('stops', []).append({'signal': int(sig), 'code': code, 'ms': (time.monotonic()-start)*1000})

    def transaction(self):
        self.setup()
        self.start_load(120)
        table = 'bulk' if self.args.tracked else 'untracked'
        rows = self.args.mb * 1_000_000 // 65536
        before = self.sql.run('SELECT pg_current_wal_insert_lsn();')[0]
        started = time.time()
        marker = str(uuid.uuid4())
        self.sql.run(f"BEGIN; INSERT INTO {table} SELECT g,0,repeat(md5(g::text),2048) FROM generate_series(1,{rows}) g; INSERT INTO circuits_source_fence VALUES ('{marker}'); COMMIT;")
        committed = time.time()
        self.record('large-commit', marker=marker, table=table, rows=rows, payload_bytes=rows*65536)
        behind = str(uuid.uuid4())
        self.sql.run(f"INSERT INTO circuits_source_fence VALUES ('{behind}');")
        behind_at = time.time()
        self.drain(marker)
        drained = time.time()
        self.drain(behind)
        self.result.update(table=table, payload_bytes=rows*65536, transaction_ms=(committed-started)*1000,
                           commit_to_receipt_ms=(drained-committed)*1000, behind_marker_ms=(time.time()-behind_at)*1000,
                           wal_bytes=int(self.sql.run(f"SELECT pg_wal_lsn_diff(pg_current_wal_insert_lsn(),'{before}');")[0]))
        self.final_compare()

    def steady(self):
        self.setup()
        self.start_load(120)
        self.final_compare()

    def catalog(self):
        self.setup()
        rec = self.feeds[0].rec
        # Exercise real API churn, then amplify that event schema while the sole
        # writer is stopped. This is a synthetic replay corpus, not a claim that
        # hundreds of MB were generated by HTTP lifecycle throughput.
        for i in range(25):
            sub = 'lab-churn-' + str(i)
            self.request('/shapes', 'POST', {'table': 'ticks', 'where':
                {'col': 'cohort', 'op': 'eq', 'value': 0}, 'subscription': sub})
            self.request('/shapes/'+rec['shapeId']+'?subscription='+sub, 'DELETE')
        self.stop_engine()
        total = pairs = 0
        while total < self.args.mb * 1_000_000:
            events = []
            for _ in range(1000):
                sub = 'lab-amplified-' + str(pairs)
                events.extend([
                    {'t': 'joined', 'id': rec['shapeId'], 'subscription': sub,
                     'at': int(time.time()), 'eid': f'lab-j-{pairs}'},
                    {'t': 'left', 'id': rec['shapeId'], 'subscription': sub, 'eid': f'lab-l-{pairs}'},
                ])
                pairs += 1
            status, _, body = http(self.dsurl+SCOPE+'meta/catalog', 'POST', events)
            if status not in (200, 204):
                raise AssertionError(f'catalog corpus append {status}: {body}')
            total += len(json.dumps(events).encode())
            self.check()
        self.result.update(catalog_fixture='25 API join/release pairs plus synthetic valid join/left history',
                           catalog_appended_bytes=total, catalog_event_pairs=pairs)
        self.start_load(100)
        start = time.time()
        self.engine, self.engineurl, boot = self.start_engine('catalog-restart')
        self.result['catalog_boot_ms'] = boot
        self.final_compare()
        self.result['restart_to_final_receipt_ms'] = (time.time()-start)*1000

    def backlog(self):
        self.setup()
        for _ in range(10):
            self.write_tick(self.sql)
        self.drain(self.commits[self.sequence][1])
        self.stop_engine()
        before = self.sql.run('SELECT pg_current_wal_insert_lsn();')[0]
        rounds = 0
        wal = 0
        # Reuse a bounded 32 MB table; checkpoint permits heap space recycling.
        self.sql.run('INSERT INTO untracked SELECT g,0,repeat(md5(g::text),2048) FROM generate_series(1,512) g;')
        while wal < self.args.mb * 1_000_000:
            self.sql.run('UPDATE untracked SET n=n+1,payload=repeat(md5(n::text || id::text),2048);')
            self.sql.run('VACUUM untracked;')
            self.write_tick(self.sql)
            rounds += 1
            wal = int(self.sql.run(f"SELECT pg_wal_lsn_diff(pg_current_wal_insert_lsn(),'{before}');")[0])
        start = time.time()
        self.engine, self.engineurl, boot = self.start_engine('restart')
        n, marker, at = self.write_tick(self.sql)
        self.drain(marker)
        self.result.update(wal_bytes=wal, rounds=rounds, boot_ms=boot, catchup_ms=(time.time()-start)*1000)
        self.final_compare()

    def restart(self):
        self.setup()
        self.watch_adapter()
        self.start_load(160)
        rng = random.Random(90210)
        cuts = []
        for i, sig in enumerate((signal.SIGTERM, signal.SIGKILL, signal.SIGTERM, signal.SIGKILL)):
            floor = self.sequence + rng.randint(8, 15)
            self.await_('cut-operation-floor', lambda: self.sequence >= floor)
            start = time.time()
            self.stop_engine(sig)
            self.engine, self.engineurl, boot = self.start_engine('restart-'+str(i))
            marker = str(uuid.uuid4())
            self.sql.run(f"INSERT INTO circuits_source_fence VALUES ('{marker}');")
            self.drain(marker)
            cuts.append({'signal': int(sig), 'operation_floor': floor, 'boot_ms': boot, 'cut_to_receipt_ms': (time.time()-start)*1000})
        self.result['cuts'] = cuts
        self.final_compare()

    def rolling(self):
        self.setup(managed=True)
        self.watch_adapter()
        self.start_load(150)
        self.await_('load-arrived', lambda: self.sequence >= 10)
        successor = str(uuid.uuid4())
        b, burl, bound = self.start_engine('B', successor, ready=False)
        standby = self.request('/_admin/deployment/status', url=burl)
        status, headers, _ = http(burl+'/shapes', 'POST', {'table': 'ticks'})
        if status != 503 or headers.get('retry-after') != '1':
            raise AssertionError(f'standby must refuse with Retry-After: {status}, {headers}')
        self.result.update(standby=standby, standby_bind_ms=bound)
        self.await_('load-during-standby', lambda: self.sequence >= 30)
        active = self.request('/_admin/deployment/status')
        start = time.time()
        self.request('/_admin/control-admission/close', 'POST')
        marker = str(uuid.uuid4())
        self.sql.run(f"INSERT INTO circuits_source_fence VALUES ('{marker}');")
        self.drain(marker)
        handoff = {'coordinationKey': active['coordinationKey'], 'ownerRevision': self.revision,
                   'generation': active['generation'], 'handoffId': str(uuid.uuid4()), 'sourceCommitId': marker}
        self.request('/_admin/deployment/quiesce', 'POST', handoff)
        self.engine.wait(timeout=50)
        self.await_('slot-released', lambda: self.sql.run("SELECT active FROM pg_replication_slots WHERE slot_name='lab_slot';") == ['f'])
        quiesced = time.time()
        self.request('/_admin/deployment/promote', 'POST', {**handoff, 'successorRevision': successor}, url=burl)
        self.engine, self.engineurl = b, burl
        self.await_('successor-ready', lambda: http(burl+'/ready')[0] == 200, 180)
        ready_at = time.time()
        marker = str(uuid.uuid4())
        self.sql.run(f"INSERT INTO circuits_source_fence VALUES ('{marker}');")
        self.drain(marker)
        self.result.update(handover_ms=(time.time()-start)*1000, old_drain_exit_ms=(quiesced-start)*1000,
                           promotion_to_ready_ms=(ready_at-quiesced)*1000,
                           successor=self.request('/_admin/deployment/status'))
        self.final_compare()

    def cleanup(self):
        self.write_stop.set()
        self.stop.set()
        self.adapter_stop.set()
        if self.adapter_thread:
            self.adapter_thread.join(timeout=5)
        if self.writer:
            self.writer.join(timeout=5)
        for f in self.feeds:
            try:
                f.close()
            except Exception as e:
                self.errors.append(str(e))
        for name, p in reversed(self.children):
            if p.poll() is None:
                try:
                    os.killpg(p.pid, signal.SIGTERM)
                    p.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    os.killpg(p.pid, signal.SIGKILL)
                    p.wait(timeout=10)
            self.record('reaped', name=name, pid=p.pid, code=p.returncode)
        if self.guard_thread:
            self.guard_thread.join(timeout=5)
        self.result['peak_created_bytes'] = max((s['created_bytes'] for s in self.samples), default=0)
        self.result['min_free_bytes'] = min((s['free_bytes'] for s in self.samples), default=0)
        self.result['engine_peak_rss_bytes'] = max((v['bytes'] for s in self.samples for v in s['rss'].values() if v['name'].startswith('engine')), default=0)
        self.result['all_children_reaped'] = all(p.poll() is not None for _, p in self.children)
        counts = {}
        outages, first_error = [], None
        for observation in self.adapter_observations:
            status = str(observation['status'])
            counts[status] = counts.get(status, 0) + 1
            if status != '200' and first_error is None:
                first_error = observation['at']
            elif status == '200' and first_error is not None:
                outages.append((observation['at']-first_error)*1000)
                first_error = None
        self.result['adapter_status_counts'] = counts
        self.result['adapter_recovered_outages_ms'] = outages
        self.result['adapter_outage_unresolved'] = first_error is not None
        (self.out/'adapter.json').write_text(json.dumps(self.adapter_observations, indent=2))
        (self.out/'samples.json').write_text(json.dumps(self.samples, indent=2))
        (self.out/'result.json').write_text(json.dumps(self.result, indent=2))
        for f in self.logs:
            f.close()
        shutil.rmtree(self.data)
        print(json.dumps(self.result, indent=2), flush=True)
        print('Artifacts:', self.out, flush=True)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('scenario', choices=['steady', 'transaction', 'backlog', 'catalog', 'restart', 'rolling'])
    p.add_argument('--engine', type=Path, required=True)
    p.add_argument('--ds', type=Path, default=Path('/Users/mh/.cargo/bin/durable-streams-server'))
    p.add_argument('--shapes', type=int, default=8)
    p.add_argument('--rate', type=float, default=10)
    p.add_argument('--mb', type=int, default=10)
    p.add_argument('--tracked', action='store_true')
    args = p.parse_args()
    if not 1 <= args.shapes <= 64 or not 0 < args.rate <= 100 or not 1 <= args.mb <= 2200:
        p.error('bounds: 1..64 shapes, 0..100 writes/s, 1..2200 MB')
    args.engine = args.engine.resolve()
    args.ds = args.ds.resolve()
    lab = Lab(args)
    def interrupt(signum, frame):
        raise KeyboardInterrupt(f'signal {signum}')
    signal.signal(signal.SIGTERM, interrupt)
    try:
        getattr(lab, args.scenario)()
        lab.result['outcome'] = 'pass'
    except BaseException as e:
        lab.result.update(outcome='fail', error=repr(e))
        raise
    finally:
        lab.cleanup()


if __name__ == '__main__':
    main()
