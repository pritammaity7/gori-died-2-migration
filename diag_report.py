"""Summarise a run's DIAG lines: one row per failing message, with a verdict.

Reads the JSONL that migrate.py writes (DIAG_FILE) and prints a table plus, when
running inside Actions, appends the same table to the job summary.

The verdict column is the whole point. `peak_mb == 0` across every download
strategy means Telegram never handed over a single byte for that file — a
source-side refusal, not a slow or flaky transfer. Anything above 0 means the
transfer started and then broke, which is a transport problem and worth
retrying. Before this existed both cases printed the identical Telethon message
("Request was unsuccessful N time(s)") and were indistinguishable.
"""
import collections
import json
import os
import sys


def load(path):
    rows = []
    with open(path, encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                pass
    return rows


def summarise(rows):
    fails = [r for r in rows if r.get('ev') in ('dl_fail', 'msg_fail')]
    agg = collections.defaultdict(
        lambda: {'n': 0, 'peak': 0.0, 'err': '', 'file': '', 'dc': None,
                 'size': 0, 'stages': set(), 'detail': ''})
    for r in fails:
        a = agg[r.get('msg_id')]
        a['n'] += 1
        a['peak'] = max(a['peak'], r.get('peak_mb') or 0)
        a['err'] = r.get('err') or a['err']
        a['file'] = r.get('file_name') or a['file']
        a['dc'] = r.get('dc') or a['dc']
        a['size'] = r.get('size') or a['size']
        a['detail'] = r.get('detail') or a['detail']
        if r.get('stage'):
            a['stages'].add(r['stage'])
    return fails, agg


def main(path):
    if not os.path.exists(path):
        print(f'no diag file at {path}')
        return 0
    rows = load(path)
    fails, agg = summarise(rows)

    print(f'diag events: {len(rows)}  download failures: {len(fails)}')
    if not fails:
        print('no download failures in this run')
    for mid, a in sorted(agg.items(), key=lambda kv: -kv[1]['n']):
        verdict = ('ZERO BYTES EVER -> unreadable at source'
                   if a['peak'] == 0 else
                   f'broke at {a["peak"]}MB -> transport, retryable')
        print(f'  msg {mid}: {a["n"]}x {a["err"]} | dc={a["dc"]} '
              f'{round((a["size"] or 0) / 1048576, 1)}MB | '
              f'{(a["file"] or "?")[:48]} | {verdict}')
        if a['stages']:
            print(f'      stages tried: {", ".join(sorted(a["stages"]))}')
        if a['detail']:
            print(f'      detail: {a["detail"][:150]}')

    for ev, label in (('refetch_ok', 'STALE REFERENCE (fixed by re-fetch)'),
                      ('refetch_gone', 'message gone at source'),
                      ('prove_ok', 'PROVED READABLE (transport problem, keep retrying)'),
                      ('prove_fail', 'PROVED UNREADABLE (zero bytes on a fresh handle)'),
                      ('prove_gone', 'PROVED GONE at source'),
                      ('dead', 'DECLARED DEAD, placeholder posted, topic unblocked'),
                      ('dead_failed', 'dead placeholder could not be posted'),
                      ('stall', 'stalled transfer'),
                      ('quota_abort', 'aborted on rate limit'),
                      ('crash', 'crashed')):
        hits = [r for r in rows if r.get('ev') == ev]
        if hits:
            ids = ', '.join(str(r.get('msg_id') or '-') for r in hits)
            print(f'  {label}: {len(hits)} ({ids})')

    # First failure's telethon tail: the real RPC error Telethon swallowed.
    if fails and fails[0].get('tl'):
        print('\n  telethon tail before the first failure:')
        for line in fails[0]['tl']:
            print(f'    {line}')
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv[1] if len(sys.argv) > 1 else '/tmp/migrate_diag.jsonl'))
