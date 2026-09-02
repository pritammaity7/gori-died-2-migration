"""Gori Died 2 -> My Courses migration worker.

Runs in GitHub Action (max ~48 min budget). Per-topic cursor state lives in the
Cloudflare panel worker's D1 database, updated after EVERY message, so a crash
never duplicates or skips anything. Order: oldest -> newest per topic.

Env: TELEGRAM_API_ID, TELEGRAM_API_HASH, TELEGRAM_SESSION (StringSession),
     OLD_CHAT_ID, NEW_CHAT_ID, WORKER_URL, MIGRATE_KEY, TIME_BUDGET_MIN
"""
import asyncio, os, sys, time, json
import logging
import re
from collections import deque
import requests
from telethon import TelegramClient, errors
from telethon.tl.functions.messages import GetForumTopicsRequest, CreateForumTopicRequest

# ===========================================================================
# DIAGNOSTICS (2026-08-30)
#
# Why this exists. Every failure of the stuck files looked like this in the log:
#
#     [FAST-DL fallback] ValueError: Request was unsuccessful 13 time(s)
#     [STD-DL attempt 1] ValueError: Request was unsuccessful 13 time(s)
#     [ERR] msg 12161 attempt 1/3: ValueError: Request was unsuccessful 13 time(s)
#
# That message is Telethon giving up after `request_retries`. It tells us NOTHING
# about the actual cause, because Telethon logs the real RPC error to its own
# logger and then raises this generic ValueError. Without the telethon logger
# attached we could not tell apart:
#
#   * a sick DC route            (transient - retry later, different account)
#   * FILE_REFERENCE_EXPIRED     (our fault - re-fetch the message)
#   * FILE_ID_INVALID / no bytes (genuinely dead at source)
#   * a 0-byte stall / timeout   (network, not Telegram)
#
# So: capture the telethon logger into a ring buffer, and on every failure emit
# ONE greppable JSON line with the full identity of the document, what we had
# actually received, and the telethon tail that preceded the give-up. Everything
# is also mirrored into the job summary + an artifact so the pattern is visible
# from the Actions page without downloading a log zip.
# ===========================================================================

DIAG_ON = os.environ.get('DIAG', '1') not in ('0', 'false', '')
TELETHON_LOG = os.environ.get('TELETHON_LOG', 'INFO').upper()
DIAG_FILE = os.environ.get('DIAG_FILE', '/tmp/migrate_diag.jsonl')

# Last N telethon log records, newest last. Telethon logs the true RPC error
# ("Telegram is having internal issues", "RpcError 400: FILE_REFERENCE_EXPIRED",
# DC migrations, timeouts) right before it raises the generic ValueError.
_tl_ring = deque(maxlen=60)


class _RingHandler(logging.Handler):
    def emit(self, record):
        try:
            _tl_ring.append(f'{record.levelname[:1]}:{record.name.split(".")[-1]}:'
                            f'{record.getMessage()[:200]}')
        except Exception:
            pass


if DIAG_ON:
    logging.basicConfig(level=logging.WARNING,
                        format='%(asctime)s %(levelname)s %(name)s %(message)s')
    _tl = logging.getLogger('telethon')
    _tl.setLevel(getattr(logging, TELETHON_LOG, logging.INFO))
    _tl.addHandler(_RingHandler())
    # keep telethon's own chatter off stdout; the ring buffer is the record
    _tl.propagate = False


def _tl_tail(n=12):
    """The telethon records that immediately preceded a failure."""
    return list(_tl_ring)[-n:]


def diag(event, **fields):
    """One greppable JSON line per interesting event.

    Format: `[DIAG] {"ev": "...", ...}` — grep the Actions log for `[DIAG]` and
    pipe through `jq` to get a table. Also appended to DIAG_FILE, which the
    workflow uploads as an artifact.
    """
    if not DIAG_ON:
        return
    rec = {'ev': event, 't': round(time.time() - START, 1), 'shard': SHARD, **fields}
    line = json.dumps(rec, ensure_ascii=False, default=str)
    print(f'[DIAG] {line}', flush=True)
    try:
        with open(DIAG_FILE, 'a', encoding='utf-8') as f:
            f.write(line + '\n')
    except OSError:
        pass


def doc_identity(msg):
    """Everything that distinguishes THIS handle to the file.

    `access_hash` is per-account and `file_reference` expires within hours, so a
    failure is only interpretable alongside these. Logged truncated — they are
    capability tokens, not secrets, but there is no reason to print them whole.
    """
    d = getattr(msg, 'document', None) or getattr(msg, 'photo', None)
    if d is None:
        return {'media': type(getattr(msg, 'media', None)).__name__}
    # file_name lives on an attribute, not on the document itself
    fn = ''
    for a in (getattr(d, 'attributes', None) or []):
        fn = getattr(a, 'file_name', '') or fn
    ref = getattr(d, 'file_reference', b'') or b''
    return {
        'doc_id': getattr(d, 'id', None),
        'dc': getattr(d, 'dc_id', None),
        'size': getattr(d, 'size', 0) or 0,
        'file_name': fn,
        'ah_tail': str(getattr(d, 'access_hash', ''))[-6:],
        'ref_len': len(ref),
        'ref_head': ref[:4].hex(),
        'mime': getattr(d, 'mime_type', ''),
    }


def _summary(md):
    """Append to the GitHub Actions job summary (visible on the run page).

    Returns True only if it was actually written, so callers do not claim to have
    produced a summary when running outside Actions.
    """
    p = os.environ.get('GITHUB_STEP_SUMMARY')
    if not p:
        return False
    try:
        with open(p, 'a', encoding='utf-8') as f:
            f.write(md + '\n')
        return True
    except OSError:
        return False


API_ID = int(os.environ['TELEGRAM_API_ID'])
API_HASH = os.environ['TELEGRAM_API_HASH']
SHARD = os.environ.get('SHARD', 'manual')


def _pick_session():
    """Each shard maps to its own Telegram account/session secret."""
    for k in (f'TELEGRAM_SESSION_{SHARD}', 'TELEGRAM_SESSION'):
        v = os.environ.get(k)
        if v and not v.startswith('PENDING'):
            return v
    print(f'[FATAL] no session secret for SHARD={SHARD}')
    sys.exit(1)


SESSION = _pick_session()
OLD_ID = int(os.environ['OLD_CHAT_ID'])
NEW_ID = int(os.environ['NEW_CHAT_ID'])
WORKER = os.environ['WORKER_URL'].rstrip('/')
# Failover host: if the primary panel is edge-rate-limited, the identical v2
# worker serves the same D1 database.
WORKER_ALT = os.environ.get('WORKER_URL_ALT', '').rstrip('/') or \
    WORKER.replace('gori-died-2-panel.', 'gori-died-2-panel-v2.')
KEY = os.environ['MIGRATE_KEY']
BUDGET_MIN = float(os.environ.get('TIME_BUDGET_MIN', '48'))
WORKER_ID = 'shard-' + SHARD + '-' + str(os.getpid())
DL_DIR = '/tmp/migdl'
START = time.time()
# A single message must never hang the whole run: hard cap (minutes-scale) plus
# an aggressive STALL detector (zero byte-growth) below.
PER_MSG_TIMEOUT = float(os.environ.get('PER_MSG_TIMEOUT', '1800'))  # 30 min hard cap
STALL_SECS = float(os.environ.get('STALL_SECS', '240'))             # no-progress limit
# Attempts (across waves) before a topic is quarantined instead of retried.
RETRY_CEILING = int(os.environ.get('RETRY_CEILING', '12'))
# Attempts before a message is declared DEAD - i.e. Telegram itself will not
# serve the bytes to anyone.
#
# Lowered from 24 to 6 on 2026-08-30. 24 was far too high: msg 12161 in topic
# 2440 sat at 5-6 attempts for the whole day and blocked its topic the entire
# time, because a blocked topic is resumed at the failure and can never pass it.
# The cost of declaring dead too early is one placeholder message; the cost of
# declaring too late is a whole course stalled indefinitely. It also gets a proof
# step now (see prove_unreadable) so the count is not the only evidence.
DEAD_AFTER = int(os.environ.get('DEAD_AFTER', '6'))
# After this many attempts, spend ONE fresh-reference probe to decide the case
# outright instead of waiting for the attempt counter to reach DEAD_AFTER.
DEAD_PROVE_AFTER = int(os.environ.get('DEAD_PROVE_AFTER', '3'))
# How long the probe is allowed to run. It only has to answer "did any byte ever
# arrive", so it does not need to complete the download.
PROVE_SECS = float(os.environ.get('PROVE_SECS', '90'))
# Telegram's real single-file ceiling for a non-Premium user account over MTProto
# is 4000 parts x 512 KB = 2,097,152,000 bytes (2000 MiB). Verified 2026-09-02
# against core.telegram.org/api/files and the saveBigFilePart part_count limit.
#
# The old cap here was 1950 MiB (2,044,723,200), i.e. 50 MiB of usable headroom
# thrown away. #65342 `Vocab Phrasal_verb practice…mp4` is 2,047,906,619 bytes =
# 1953.0 MiB: over the old cap by only 3.04 MiB, but 47 MiB UNDER what Telegram
# actually accepts. It was refused for no reason.
#
# Set to the true limit, minus a 4 MiB safety margin for the attribute/caption
# overhead that rides along with the media.
UPLOAD_MAX = int(os.environ.get(
    'UPLOAD_MAX_BYTES', str(4000 * 512 * 1024 - 4 * 1024 * 1024)))


try:
    import fast_telethon as FT          # vendored parallel-transfer module
except Exception:
    FT = None                           # silently degrade to standard transfers

HDRS = {'x-migrate-key': KEY, 'content-type': 'application/json'}

# Hard client-side budget. On 2026-08-29 a spin loop spent 136,898 Worker
# requests and 5.1 BILLION D1 rows read in a single day (limits: 100k requests,
# 5M rows). A worker must never be *able* to do that again, whatever the bug.
CALL_BUDGET = int(os.environ.get('CALL_BUDGET', '2000'))
_calls = {'n': 0, 'rate_limited': 0}

# QUOTA GUARD (2026-08-30). When the account's daily Worker quota is exhausted
# every panel call returns Cloudflare's 429. Previously the worker kept going and
# recorded messages it had never even attempted as "failed" - six phantom
# failures were created that way in the last 20 minutes before a quota reset, and
# each carried a fake 12-attempt history toward the DEAD threshold.
# Now: after this many consecutive 429s the run stops cleanly. No progress is
# invented, nothing is marked failed, and the next wave resumes from the cursor.
RATE_LIMIT_ABORT = int(os.environ.get('RATE_LIMIT_ABORT', '3'))

# The panel answers a Workers-request overrun with HTTP 429, but a D1 ROW-limit
# overrun with HTTP 500 and this text in the body. Both mean "the account is out
# of quota, nothing you retry will work"; only the first was recognised, so on
# 2026-09-01 every wave from 14:00 UTC onward spent its whole hour retrying
# /api/claim six times per call and achieved nothing. Match on the body too.
QUOTA_BODY = re.compile(
    r'row read limit|rows read limit|exceeded .{0,40}free tier|D1_ERROR.*limit',
    re.I)


def _is_quota_body(text):
    return bool(text) and bool(QUOTA_BODY.search(text[:400]))


class QuotaExhausted(RuntimeError):
    """Panel cannot serve us: account-wide quota (requests or D1 rows) is gone.

    Raised so the run stops cleanly instead of inventing state. Without the panel
    we cannot tell done from not-done, and guessing is what produced phantom
    failures on 2026-08-30.
    """


def budget_left():
    return CALL_BUDGET - _calls['n']


def api_get(path, tries=6):
    """GET with backoff and failover.

    A single transient 429 from Cloudflare's edge used to kill an entire run
    (exit code 1) because this had no retry at all - that was the cause of five
    dead waves on 2026-08-29. Never raise on a transient; the caller decides.
    """
    hosts = [WORKER] + [h for h in (WORKER_ALT,) if h and h != WORKER]
    for attempt in range(tries):
        host = hosts[attempt % len(hosts)]
        try:
            r = requests.get(host + path, headers=HDRS, timeout=30)
            if r.status_code == 200:
                return r.json()
            print(f'   [GET {path}] HTTP {r.status_code} (attempt {attempt + 1}/{tries})')
            if r.status_code == 429 or _is_quota_body(r.text):
                # account-level quota: backing off harder is the only sane move
                time.sleep(30)
        except Exception as e:
            print(f'   [GET {path}] {type(e).__name__} (attempt {attempt + 1}/{tries})')
        time.sleep(min(60, 4 * (attempt + 1) ** 2))
    print(f'   [WARN] {path} unreachable after {tries} tries')
    return {}


def api_post(path, payload, tries=6):
    if _calls['n'] >= CALL_BUDGET:
        print(f'   [BUDGET] {CALL_BUDGET} panel calls used - refusing further calls')
        return {}
    _calls['n'] += 1
    hosts = [WORKER] + [h for h in (WORKER_ALT,) if h and h != WORKER]
    saw_429 = False
    quota_reason = ''
    for attempt in range(tries):
        host = hosts[attempt % len(hosts)]
        try:
            r = requests.post(host + path, headers=HDRS, data=json.dumps(payload), timeout=30)
            if r.status_code == 200:
                _calls['rate_limited'] = 0        # healthy again
                return r.json()
            print(f'   [POST {path}] HTTP {r.status_code} (attempt {attempt + 1}/{tries})')
            if r.status_code == 429:
                saw_429 = True
                quota_reason = quota_reason or 'HTTP 429 (Workers request limit)'
                time.sleep(20)
            elif _is_quota_body(r.text):
                # HTTP 500 carrying D1's row-limit message. Same meaning as a 429:
                # the account is out of quota until 00:00 UTC. Retrying is futile.
                saw_429 = True
                quota_reason = quota_reason or f'HTTP {r.status_code} D1 row limit'
                print(f'   [QUOTA] D1 row limit reached: {r.text[:120]}')
                break
        except Exception as e:
            print(f'   [POST {path}] {type(e).__name__} (attempt {attempt + 1}/{tries})')
        time.sleep(min(60, 4 * (attempt + 1) ** 2))

    if saw_429:
        # Account-wide quota, not a per-request hiccup. Refusing to continue is
        # essential: without state we cannot tell done from not-done, and guessing
        # is what produced phantom failures before.
        _calls['rate_limited'] += 1
        if _calls['rate_limited'] >= RATE_LIMIT_ABORT:
            raise QuotaExhausted(
                f'panel out of quota on {_calls["rate_limited"]} consecutive calls '
                f'({quota_reason})')
    print(f'   [WARN] {path} unreachable, continuing (state may replay)')
    return {}


def remaining():
    return BUDGET_MIN * 60 - (time.time() - START)


def topic_is_forum_topic(m):
    """True if this message lives inside a named forum topic (not the General area)."""
    rt = getattr(m, 'reply_to', None)
    if rt is None:
        return False
    top = getattr(rt, 'reply_to_top_id', None)
    if top:
        return True
    rid = getattr(rt, 'reply_to_msg_id', 0)
    # bare reply_to_msg_id==1 (or a service marker) means the General area
    return rid not in (0, 1)


async def _run_monitored(label, coro, path):
    """Drive a download coroutine while watching byte growth of `path`.
    Zero new bytes for STALL_SECS -> cancel and raise RuntimeError so the
    message is marked FAILED and retried next wave (never hangs the run).
    Steady large transfers are unaffected regardless of duration.

    Also records the byte-growth curve, which is the only way to tell a file that
    never started (0 bytes -> Telegram refused) from one that died mid-transfer
    (progress then stall -> DC/network). That distinction was invisible before:
    both surfaced as the same generic ValueError.
    """
    task = asyncio.create_task(coro)
    last, last_chg = -1, time.time()
    peak = 0
    ticks = 0
    try:
        while True:
            done, _ = await asyncio.wait({task}, timeout=20)
            if task in done:
                return task.result()
            try:
                cur = os.path.getsize(path)
            except OSError:
                cur = 0
            peak = max(peak, cur)
            ticks += 1
            if cur != last:
                last, last_chg = cur, time.time()
            elif time.time() - last_chg > STALL_SECS:
                task.cancel()
                try:
                    await task
                except Exception:
                    pass
                diag('stall', label=label, peak_mb=round(peak / 1048576, 2),
                     ticks=ticks, stall_secs=STALL_SECS, tl=_tl_tail())
                raise RuntimeError(f'{label}: stalled {STALL_SECS:.0f}s '
                                   f'(zero progress at {cur / 1048576:.0f}MB)')
    except BaseException:
        if not task.done():
            task.cancel()
        raise
    finally:
        # `peak` is what makes a failure readable: 0 means Telegram never sent a
        # byte, >0 means the transfer started and then broke.
        # Sample once more here: the poll loop only looks every 20s, so a task
        # that failed quickly would otherwise report a false 0 and be
        # misclassified as "Telegram refused".
        try:
            peak = max(peak, os.path.getsize(path))
        except OSError:
            pass
        _run_monitored.last_peak = peak


_run_monitored.last_peak = 0


async def smart_download(client, msg, path):
    """Download with three escalating strategies.

    What the logs actually show for a repeated failure:
        ValueError: Request was unsuccessful 13 time(s)
    That is Telethon's *give-up* message after exhausting `request_retries` on a
    GetFileRequest. It names no cause. A sick DC route, an expired
    `file_reference`, a genuinely unreadable file and a plain timeout all produce
    that identical line, which is why earlier diagnoses of it were guesses. The
    telethon logger (attached above, ring-buffered) carries the real RPC error and
    is emitted with every `dl_fail` record.

    The strategies escalate because the *transient DC* case is the common one and
    is genuinely recoverable:
      1. FastTelethon parallel download (many senders, fastest)
      2. standard download with a smaller chunk size (gentler on a sick DC)
      3. standard download after a cooldown, giving the DC time to recover
      4. FRESH-DL - re-fetch the message for a new file_reference, which both
         fixes and *proves* the stale-handle case
    Only if all of those fail does the caller record a failure - and even then the
    message is never skipped, it just blocks its own topic until it succeeds.
    """
    doc = getattr(msg, 'document', None)
    size = (doc.size if doc else 0) or 0
    ident = doc_identity(msg)

    if FT is not None and doc is not None and size >= 5 * 1024 * 1024:
        t0 = time.time()

        async def run_fast():
            with open(path, 'wb') as f:
                await FT.download_file(client, doc, f)
            got = os.path.getsize(path)
            if got != size:
                raise RuntimeError(f'SIZE MISMATCH {got} != {size}')
            return path

        try:
            p = await _run_monitored('FAST-DL', run_fast(), path)
            print(f'   [FAST-DL] {size / 1024 / 1024:.1f}MB in {time.time() - t0:.0f}s')
            return p
        except Exception as e:
            print(f'   [FAST-DL fallback] {type(e).__name__}: {str(e)[:90]}')
            diag('dl_fail', stage='FAST-DL', msg_id=msg.id,
                 err=type(e).__name__, detail=str(e)[:200],
                 peak_mb=round(_run_monitored.last_peak / 1048576, 2),
                 secs=round(time.time() - t0, 1), **ident, tl=_tl_tail())
            try:
                os.remove(path)
            except OSError:
                pass

    # 256 KB parts: legal per MTProto (1048576 % 262144 == 0) and far more
    # tolerant of a DC that is timing out on 1 MB reads.
    #
    # NOTE: part_size_kb belongs to download_file(), NOT download_media().
    # Telethon 1.44's download_media signature is
    # (message, file, thumb, progress_callback) - passing part_size_kb raised
    # TypeError on EVERY fallback attempt, so once FAST-DL failed the message
    # could never succeed and blocked its topic forever. That is exactly what
    # 22279 / 11813 / 79731 / 20220 / 12161 were doing. Use iter_download, which
    # does take request_size, and write the file ourselves.
    for attempt, (part, cooldown) in enumerate(((256 * 1024, 0), (128 * 1024, 45)), 1):
        if cooldown:
            print(f'   [DL COOLDOWN] waiting {cooldown}s for the DC to recover')
            await asyncio.sleep(cooldown)

        async def run_std():
            with open(path, 'wb') as f:
                async for chunk in client.iter_download(msg, request_size=part):
                    f.write(chunk)
            if size:
                got = os.path.getsize(path)
                if got != size:
                    raise RuntimeError(f'SIZE MISMATCH {got} != {size}')
            return path

        t1 = time.time()
        try:
            return await _run_monitored(f'STD-DL/{part // 1024}k', run_std(), path)
        except Exception as e:
            print(f'   [STD-DL attempt {attempt}] {type(e).__name__}: {str(e)[:90]}')
            diag('dl_fail', stage=f'STD-DL/{part // 1024}k', msg_id=msg.id,
                 attempt=attempt, err=type(e).__name__, detail=str(e)[:200],
                 peak_mb=round(_run_monitored.last_peak / 1048576, 2),
                 secs=round(time.time() - t1, 1), **ident, tl=_tl_tail())
            try:
                os.remove(path)
            except OSError:
                pass
            if attempt == 2:
                # FRESH-REFERENCE LAST RESORT.
                #
                # `file_reference` expires within hours and `access_hash` is
                # per-account. A message object that has been sitting in an
                # iterator for most of a 48-minute run can therefore hold a
                # handle Telegram no longer honours - and Telethon reports that
                # as the same opaque "Request was unsuccessful N time(s)" as a
                # sick DC. Re-fetching the message costs one API call and either
                # succeeds (it was a stale handle) or proves the file is dead.
                # Either way the log now says WHICH.
                try:
                    fresh = (await client.get_messages(msg.peer_id, ids=[msg.id]))[0]
                except Exception as fe:
                    diag('refetch_fail', msg_id=msg.id, err=type(fe).__name__,
                         detail=str(fe)[:200], tl=_tl_tail())
                    raise e
                if fresh is None or not fresh.media:
                    diag('refetch_gone', msg_id=msg.id,
                         note='message or media no longer visible at source')
                    raise e
                new_ident = doc_identity(fresh)
                diag('refetch', msg_id=msg.id, old=ident, new=new_ident,
                     ref_changed=new_ident.get('ref_head') != ident.get('ref_head'))

                async def run_fresh():
                    with open(path, 'wb') as f:
                        async for chunk in client.iter_download(fresh,
                                                                request_size=256 * 1024):
                            f.write(chunk)
                    return path

                t2 = time.time()
                try:
                    p = await _run_monitored('FRESH-DL', run_fresh(), path)
                    diag('refetch_ok', msg_id=msg.id,
                         secs=round(time.time() - t2, 1),
                         note='STALE FILE REFERENCE was the cause, not a dead file')
                    return p
                except Exception as e2:
                    diag('dl_fail', stage='FRESH-DL', msg_id=msg.id,
                         err=type(e2).__name__, detail=str(e2)[:200],
                         peak_mb=round(_run_monitored.last_peak / 1048576, 2),
                         secs=round(time.time() - t2, 1), **new_ident,
                         tl=_tl_tail(),
                         note='failed even with a FRESH reference -> source-side')
                    try:
                        os.remove(path)
                    except OSError:
                        pass
                    raise


async def prove_unreadable(client, peer, msg_id):
    """Decide, in one shot, whether Telegram will serve ANY byte of this message.

    Called once a message has failed DEAD_PROVE_AFTER times. The attempt counter
    alone is weak evidence - it counts our failures, not Telegram's refusal - and
    waiting for it to climb to DEAD_AFTER leaves the whole topic blocked in the
    meantime. This gets a definitive answer instead:

      * re-fetch the message so the file_reference and access_hash are fresh,
        removing the single most common false positive
      * ask for only the FIRST chunk. If Telegram is willing to serve the file at
        all, the first chunk arrives quickly; if it refuses, it refuses here too.

    Returns (verdict, detail):
      'readable'  - bytes arrived; this is a transport problem, keep retrying
      'unreadable'- fresh handle, still zero bytes; safe to declare dead
      'gone'      - the message or its media no longer exists at source
      'unknown'   - we could not run the probe (flood wait, no media, etc.)
    """
    try:
        fresh = (await client.get_messages(peer, ids=[msg_id]))[0]
    except Exception as e:
        diag('prove_error', msg_id=msg_id, err=type(e).__name__,
             detail=str(e)[:200], tl=_tl_tail())
        return 'unknown', f'{type(e).__name__}: {str(e)[:120]}'
    if fresh is None or not fresh.media:
        diag('prove_gone', msg_id=msg_id)
        return 'gone', 'message or media no longer visible at source'

    ident = doc_identity(fresh)
    got = 0
    t0 = time.time()
    size = fresh.document.size if fresh.document else 0
    # A one-chunk probe is not enough. #10465 (1038 MB) served its first 256 KB
    # instantly and then crawled at 0.22 MB/s - 43 MB in 182 s, which is ~80
    # minutes for the whole file and longer than the entire run budget. The old
    # probe called that 'readable' and would retry it forever; the old counter
    # called it dead and threw it away. Both were wrong.
    #
    # So measure THROUGHPUT, not liveness: read for PROVE_SECS and work out
    # whether the file could finish inside one run at the rate observed.
    try:
        async def sample():
            nonlocal got
            async for chunk in client.iter_download(fresh, request_size=1024 * 1024):
                got += len(chunk)
                if time.time() - t0 >= PROVE_SECS or got >= size:
                    break
        await asyncio.wait_for(sample(), timeout=PROVE_SECS * 1.5)
    except Exception as e:
        diag('prove_fail', msg_id=msg_id, err=type(e).__name__,
             detail=str(e)[:200], got=got, secs=round(time.time() - t0, 1),
             tl=_tl_tail(), **ident)
        return ('unreadable' if got == 0 else 'slow'), \
            f'{type(e).__name__} after {got} bytes'

    el = max(0.001, time.time() - t0)
    rate = got / el
    eta = (size / rate) if rate else float('inf')
    ident.update(got=got, secs=round(el, 1),
                 rate_mbs=round(rate / 1048576, 3), eta_min=round(eta / 60, 1))

    if got == 0:
        diag('prove_fail', msg_id=msg_id, note='fresh handle served nothing',
             **ident)
        return 'unreadable', 'zero bytes from a fresh reference'
    if got >= size:
        diag('prove_ok', msg_id=msg_id, note='whole file read during probe',
             **ident)
        return 'readable', f'complete, {got} bytes in {el:.0f}s'
    # Could it finish inside the remaining run time, with slack for the upload?
    if eta > max(600.0, remaining() * 0.5):
        diag('prove_slow', msg_id=msg_id,
             note='serves bytes but far too slowly to complete in one run',
             **ident)
        return 'slow', (f'{rate/1048576:.2f} MB/s -> ETA {eta/60:.0f} min for '
                        f'{size/1048576:.0f} MB')
    diag('prove_ok', msg_id=msg_id, note='healthy throughput', **ident)
    return 'readable', f'{rate/1048576:.2f} MB/s, ETA {eta/60:.0f} min'


class MsgBatch:
    """Buffers per-message updates, flushes via /api/bulk.
    - flush at >=10 items or 30s age (keeps Cloudflare/D1 daily limits safe)
    - expensive transfers flush immediately for crash-durability
    - always flush before releasing a topic"""

    BATCH_N = 10
    BATCH_SECS = 30
    BIG = 5 * 1024 * 1024

    def __init__(self, topic_root):
        self.topic_root = topic_root
        self.items = []
        self.last = time.time()

    def add(self, item):
        self.items.append(item)

    def is_big(self, item):
        return (item.get('size') or 0) >= self.BIG or \
               item.get('kind') in ('video', 'document', 'photo', 'poll')

    async def maybe_flush(self, item=None):
        if item is not None and self.is_big(item):
            await self.flush()
            return
        if len(self.items) >= self.BATCH_N or (self.items and time.time() - self.last >= self.BATCH_SECS):
            await self.flush()

    async def flush(self):
        if not self.items:
            return
        n = len(self.items)
        r = api_post('/api/bulk', {'topic_root': self.topic_root, 'items': self.items})
        self.items.clear()
        self.last = time.time()
        if not r.get('ok'):
            print(f'   [BULK WARN] {n} updates may need replay: {str(r)[:120]}')
        else:
            print(f'   [BULK] {n} updates flushed')


async def fast_upload_and_send(client, peer, path, src_msg, reply_to, caption):
    """Parallel upload for documents/videos >=20MB. Preserves EXACT source
    attributes/mime-type/thumbnail by handing the pre-uploaded InputFile to
    send_file (which builds the message correctly). Raises if not applicable -
    caller then uses the standard send path."""
    doc = getattr(src_msg, 'document', None)
    size = os.path.getsize(path)
    if FT is None or not doc or size < 20 * 1024 * 1024:
        raise RuntimeError('fast upload not applicable')
    fn = next((getattr(a, 'file_name', '') for a in doc.attributes
               if hasattr(a, 'file_name')), '') or 'file'
    t0 = time.time()
    with open(path, 'rb') as f:
        input_file = await FT.upload_file(client, f, filename=fn)
    thumb = None
    try:  # original thumbnail -> identical preview in the new topic
        if doc.thumbs:
            tp = await client.download_media(src_msg, thumb=-1)
            if tp:
                thumb = await client.upload_file(tp)
                os.remove(tp)
    except Exception:
        thumb = None
    sent = await client.send_file(
        peer, input_file,
        caption=(caption or None), reply_to=reply_to,
        attributes=list(doc.attributes), mime_type=doc.mime_type,
        force_document=True, file_name=fn, thumb=thumb)
    print(f'   [FAST-UP] {size/1024/1024:.1f}MB in {time.time()-t0:.0f}s')
    return sent


def is_video(doc):
    return any(type(a).__name__ == 'DocumentAttributeVideo' for a in doc.attributes)


async def seed_topics(client, old):
    """Fetch all topics from old group, keep CLOSED ones only, push to D1."""
    topics, offset_topic, offset_date, offset_id = [], 0, None, 0
    while True:
        res = await client(GetForumTopicsRequest(
            peer=old, offset_date=offset_date, offset_id=offset_id,
            offset_topic=offset_topic, limit=100))
        if not res.topics:
            break
        topics.extend(res.topics)
        offset_topic = res.topics[-1].id
        offset_date = getattr(res.topics[-1], 'date', None)
        offset_id = res.topics[-1].top_message
        if len(res.topics) < 100:
            break
    closed = [{'root_id': t.id, 'title': getattr(t, 'title', f'topic-{t.id}'),
               'closed': True, 'top_msg': t.top_message or 0}
              for t in topics if t.closed]
    print(f'[SEED] {len(topics)} topics total, {len(closed)} closed -> migrating these')
    api_post('/api/seed', {'topics': closed})
    return closed


async def ensure_new_topic(client, new, row):
    """Create matching topic in new group once; returns new root msg id."""
    if row.get('new_topic_id'):
        return row['new_topic_id']
    res = await client(CreateForumTopicRequest(
        peer=new, title=row['title'],
        random_id=int.from_bytes(os.urandom(8), 'big', signed=True)))
    new_id = None
    for u in res.updates:
        msg = getattr(u, 'message', None)
        if msg is not None:
            new_id = msg.id
            break
    print(f'   [TOPIC] created {row["title"]!r} -> root {new_id}')
    api_post('/api/update', {'topic_root': row['old_root'], 'cursor': row.get('cursor', 0),
                             'new_topic_id': new_id})
    return new_id

async def process_message(client, new, m, new_tid, row):
    """Copy one old message into the new topic. Returns (status, new_msg_id, meta)."""
    meta = {'old_msg_id': m.id, 'caption': (m.message or ''), 'kind': 'text',
            'file_name': '', 'size': 0}
    if m.action and not m.media:
        return 'skipped', None, meta  # service messages (joins, pins, edits)

    text = m.message or ''
    mtype = type(m.media).__name__ if m.media else ''

    if m.document:
        doc = m.document
        size = doc.size or 0
        fn = next((getattr(a, 'file_name', '') for a in doc.attributes
                   if hasattr(a, 'file_name')), '')
        meta.update(kind='video' if is_video(doc) else 'document',
                    file_name=fn, size=size)
        if size > UPLOAD_MAX:
            # Genuinely beyond what Telegram will accept (see UPLOAD_MAX).
            #
            # Recorded as 'dead', not 'skipped'. Two reasons, both found
            # 2026-09-02: 'skipped' also means "service message, nothing to
            # copy", so overloading it hid real losses; and the site lists
            # `status IN ('done','dead')`, so a dead row with a placeholder shows
            # up in the course with its reason instead of leaving a silent hole.
            # #65342 in ENGLISH SPL 1 vanished exactly this way.
            why = (f'{size / 1048576:.0f} MB is over Telegram\'s '
                   f'{UPLOAD_MAX / 1048576:.0f} MB single-file upload limit')
            meta['caption'] = why
            try:
                ph = await client.send_message(
                    new, f'⚠️ Could not copy: {fn or f"message {m.id}"}\n'
                         f'{why}. It has to be split or uploaded by hand. '
                         f'Everything before and after it is complete and in '
                         f'order.',
                    reply_to=new_tid)
                meta['placeholder'] = ph.id
                print(f'   [TOO BIG] {why}; placeholder {ph.id} in position')
                return 'dead', ph.id, meta
            except Exception as e:
                print(f'   [TOO BIG] placeholder failed: '
                      f'{type(e).__name__}: {str(e)[:80]}')
                return 'dead', None, meta
        os.makedirs(DL_DIR, exist_ok=True)
        safe_fn = ''.join(c for c in (fn or f'file_{m.id}') if c not in '\\/:*?"<>|').strip() or f'file_{m.id}'
        final_path = os.path.join(DL_DIR, safe_fn)
        path = await smart_download(client, m, final_path)
        if path:
            try:
                vid = is_video(doc)
                try:
                    sent = await fast_upload_and_send(
                        client, new, path, m, new_tid, text)
                except Exception as ue:
                    if 'not applicable' not in str(ue):
                        print(f'   [FAST-UP fallback] {type(ue).__name__}: {str(ue)[:90]}')
                    sent = await client.send_file(
                        new, path, caption=(text or None),
                        reply_to=new_tid,
                        supports_streaming=vid, force_document=not vid)
                return 'done', sent.id, meta
            finally:
                try:
                    os.remove(path)
                except OSError:
                    pass
    elif mtype == 'MessageMediaWebPage':
        pass  # just a link preview - send as plain text below
    elif mtype == 'MessageMediaPoll':
        poll = m.media.poll
        q = ''
        try:
            q = poll.question.text if hasattr(poll.question, 'text') else str(poll.question)
        except Exception:
            q = ''
        out = (text + f'\n[poll: {q}]').strip() or f'[poll: {q}]'
        sent = await client.send_message(
            new, out, reply_to=new_tid)
        meta.update(kind='poll', caption=out)
        return 'done', sent.id, meta
    elif m.media:
        os.makedirs(DL_DIR, exist_ok=True)
        nm = getattr(getattr(m, 'file', None), 'name', None) or f'media_{m.id}'
        safe_nm = ''.join(c for c in nm if c not in '\\/:*?"<>|').strip() or f'media_{m.id}'
        path = await client.download_media(m, file=os.path.join(DL_DIR, safe_nm))
        if path:
            try:
                sent = await client.send_file(
                    new, path, caption=(text or None),
                    reply_to=new_tid)
                meta['kind'] = 'photo' if mtype == 'MessageMediaPhoto' else mtype
                return 'done', sent.id, meta
            finally:
                try:
                    os.remove(path)
                except OSError:
                    pass

    text = text.strip()
    if not text:
        return 'skipped', None, meta  # nothing copyable in this message
    sent = await client.send_message(
        new, text, formatting_entities=m.entities or None,
        reply_to=new_tid)
    return 'done', sent.id, meta

async def count_totals(client, old, root):
    """One-time walk: count copyable messages + media in a topic (for 'left' counters)."""
    total = media = 0
    async for m in client.iter_messages(old, reply_to=root, reverse=True):
        if m.action and not m.media:
            continue
        total += 1
        if m.document or m.photo:
            media += 1
    return total, media


async def count_totals_full(client, old, root):
    """Extended walk: also sums bytes and splits video/doc/photo counts."""
    total = media = videos = docs = photos = 0
    total_bytes = 0
    async for m in client.iter_messages(old, reply_to=root, reverse=True):
        if m.action and not m.media:
            continue
        total += 1
        if m.document:
            media += 1
            total_bytes += m.document.size or 0
            if is_video(m.document):
                videos += 1
            else:
                docs += 1
        elif m.photo:
            media += 1
            photos += 1
    return {'total_msgs': total, 'total_media': media, 'total_bytes': total_bytes,
            'total_videos': videos, 'total_docs': docs, 'total_photos': photos}


async def main():
    # Stagger starts: five shards launching within a couple of seconds hammered
    # the panel hard enough to trip Cloudflare's edge limiter. Shard index gives
    # each worker its own slot.
    try:
        slot = int(SHARD) * 8
    except ValueError:
        slot = 0
    if slot:
        print(f'[STAGGER] shard {SHARD}: waiting {slot}s before startup')
        await asyncio.sleep(slot)

    client = TelegramClient(StringSessionHolder(), API_ID, API_HASH,
                            # Transient DC timeouts were the top cause of failed
                            # messages: Telethon gave up after 6 tries and raised
                            # ValueError. More retries + a longer per-request
                            # timeout let these files through instead.
                            request_retries=12,
                            connection_retries=None,
                            retry_delay=5,
                            timeout=90,
                            flood_sleep_threshold=120,
                            auto_reconnect=True)
    await client.connect()
    if not await client.is_user_authorized():
        print('[FATAL] session invalid')
        sys.exit(1)
    me = await client.get_me()
    acct = f"{(me.first_name or '?').strip()[:14]}|{me.phone or me.id}"
    global WORKER_ID
    WORKER_ID = f'{acct}-s{SHARD}-{os.getpid()}'[:80]
    print(f'[ACCOUNT] {acct} | shard={SHARD}')
    old = await client.get_entity(OLD_ID)
    new = await client.get_entity(NEW_ID)
    print(f'[OK] {me.first_name} | worker={WORKER_ID} | budget {BUDGET_MIN} min')

    # ALWAYS re-seed: refreshes top_msg for known topics (catches new messages
    # appended to old topics) and discovers newly-closed topics (patrol mode).
    # NOTE: /api/state used to be called twice here and its result thrown away -
    # ten of the heaviest requests per wave for nothing. Removed.
    await seed_topics(client, old)

    topics_done = 0
    idle_rounds = 0
    while remaining() > 300:
        c = api_post('/api/claim', {'worker_id': WORKER_ID})
        t = c.get('topic')
        if not t:
            print('[CLAIM] nothing claimable right now (all done or quarantined)')
            idle_rounds += 1
            if idle_rounds >= 3 or remaining() < 600:
                break
            # never spin: a claim loop with no cooldown is what tripped the edge
            # limiter. Wait before asking again.
            await asyncio.sleep(60)
            continue
        idle_rounds = 0
        root = t['old_root']
        cursor = t.get('cursor') or 0
        done_ids = set()          # panel no longer ships id sets; cursor is truth
        failed_ids = set()
        retry_from = c.get('retry_from')
        retry_tries = c.get('retry_tries') or 0
        is_general = (root == -1)   # special row: whole-group chat outside topics
        # An unresolved failure means this topic is BLOCKED at that message. We
        # must resume exactly there - never past it - because the destination
        # only appends.
        if retry_from is not None:
            # PROOF BEFORE PATIENCE, AND PROOF EVEN AT THE CEILING.
            #
            # The old logic only counted attempts: retry until 24, then declare
            # dead. That is why 12161 blocked topic 2440 all day - the counter
            # crawled while the topic made zero progress, and the count itself
            # never distinguished "Telegram refuses this file" from "our route was
            # unlucky". Now, once a message has failed DEAD_PROVE_AFTER times, we
            # spend one cheap probe to settle it.
            #
            # 2026-09-02 fix, in two parts.
            #
            # (a) The probe used to be skipped at exactly
            # retry_tries >= DEAD_AFTER (`and retry_tries < DEAD_AFTER`), so the
            # attempt counter alone still killed a file the moment it hit 6 -
            # the exact defect the probe was added to remove. #10465
            # (26. Plane Geometry Class 26, 1038 MB) died that way.
            #
            # (b) The probe itself was too weak: one 256 KB chunk. #10465 serves
            # that chunk immediately and then crawls at 0.22 MB/s (43 MB in
            # 182 s measured, ~80 min for the whole file), so "first chunk
            # arrived" proved nothing. prove_unreadable() now measures
            # throughput and returns a third verdict, 'slow'.
            #
            # 'slow' is neither dead nor retryable: retrying burns a whole run
            # for nothing, and declaring it dead throws away a file that does
            # exist. It gets a placeholder in position (so no hole) and the topic
            # moves on, with the reason recorded so a human can forward it by
            # hand. 'unknown' still falls through to the counter.
            verdict = detail = None
            if retry_tries >= DEAD_PROVE_AFTER:
                verdict, detail = await prove_unreadable(
                    client, old, retry_from)
                print(f'   [PROVE] old_msg {retry_from} after {retry_tries} '
                      f'attempts: {verdict} ({detail})')
                diag('prove', topic=root, title=t.get('title'),
                     msg_id=retry_from, tries=retry_tries,
                     verdict=verdict, detail=detail)
            # 'gone', 'unreadable' and 'slow' are all terminal for the fleet:
            # no further automated attempt can succeed. Otherwise the counter may
            # only give up on a file the probe has not vouched for.
            if verdict in ('unreadable', 'gone', 'slow') or (
                    retry_tries >= DEAD_AFTER and verdict != 'readable'):
                # No retry path can work. Post a placeholder IN POSITION so the
                # sequence keeps its slot, record the message as dead, and let the
                # topic continue. No deletion, no replay - the position is
                # occupied, so there is no hole.
                info = api_post('/api/deadinfo', {'topic_root': root,
                                                  'old_msg_id': retry_from}) or {}
                name = info.get('file_name') or ''
                why = ('no longer present at source' if verdict == 'gone' else
                       'Telegram served zero bytes from a fresh reference'
                       if verdict == 'unreadable' else
                       f'too slow to transfer: {detail}' if verdict == 'slow' else
                       f'{retry_tries} failed attempts')
                # The notice must not claim "no longer serves this file" for a
                # 'slow' verdict - the file is there, it just will not move fast
                # enough for an automated run. Saying otherwise is what made the
                # #10465 placeholder misleading.
                body = (f'⚠️ Missing file: {name or f"message {retry_from}"}\n'
                        f'Telegram serves this file too slowly to copy '
                        f'automatically ({detail}), so it needs to be forwarded '
                        f'by hand. Everything before and after it is complete '
                        f'and in order.'
                        if verdict == 'slow' else
                        f'⚠️ Missing file: {name or f"message {retry_from}"}\n'
                        f'Telegram no longer serves this file from the source '
                        f'channel, so it could not be copied. Everything before '
                        f'and after it is complete and in order.')
                try:
                    ph = await client.send_message(
                        new, body,
                        reply_to=await ensure_new_topic(client, new, t))
                    api_post('/api/dead', {
                        'topic_root': root, 'old_msg_id': retry_from,
                        'new_msg_id': ph.id, 'file_name': name,
                        'size': info.get('size') or 0, 'kind': info.get('kind') or 'unknown',
                        'reason': f'{why} (after {retry_tries} attempts)'})
                    print(f'   [DEAD] old_msg {retry_from} marked dead ({why}); '
                          f'placeholder {ph.id} posted in position')
                    diag('dead', topic=root, title=t.get('title'),
                         msg_id=retry_from, tries=retry_tries, verdict=verdict,
                         reason=why, placeholder=ph.id, file_name=name)
                    _summary(f'- **DEAD** `{t.get("title")}` old_msg `{retry_from}` '
                             f'({name or "?"}): {why}. Placeholder posted, topic '
                             f'unblocked.')
                    continue
                except Exception as e:
                    print(f'   [DEAD] placeholder failed: {type(e).__name__}: {str(e)[:80]}')
                    diag('dead_failed', topic=root, msg_id=retry_from,
                         err=type(e).__name__, detail=str(e)[:200])
                    api_post('/api/quarantine', {'topic_root': root})
                    continue
            if retry_tries >= RETRY_CEILING:
                # Not yet provably dead: quarantine with exponential cooldown so
                # the fleet works elsewhere and the panel is never hammered.
                print(f"[QUARANTINE] {t['title']!r} stuck at old_msg {retry_from} "
                      f"after {retry_tries} attempts")
                q = api_post('/api/quarantine', {'topic_root': root})
                print(f"   cooldown {q.get('cooldown_secs', '?')}s")
                continue
            cursor = min(cursor, retry_from - 1)
            print(f'   [RESUME] unresolved failure at {retry_from} '
                  f'({retry_tries} prior attempts) - restarting from {cursor + 1}')
            diag('resume_blocked', topic=root, title=t.get('title'),
                 retry_from=retry_from, prior_tries=retry_tries,
                 ceiling=RETRY_CEILING, dead_after=DEAD_AFTER)
        print(f"\n=== CLAIMED {t['title']!r} (root={root}, cursor={cursor}, "
              f"calls_used={_calls['n']}/{CALL_BUDGET}) ===")

        try:
            new_tid = await ensure_new_topic(client, new, t)
            if not new_tid:
                print('   [WARN] no new topic id, releasing')
                continue

            batch = MsgBatch(root)

            if t.get('total_msgs') is None:
                if remaining() < 600:
                    print('[TIME] not enough budget to scan a new topic, releasing')
                    continue
                stats = await count_totals_full(client, old, root)
                api_post('/api/totals', {'topic_root': root, **stats})
                print(f"   [COUNT] {stats['total_msgs']} copyable msgs, "
                      f"{stats['total_media']} media, {stats['total_bytes']/1073741824:.2f} GB "
                      f"({stats['total_videos']} videos / {stats['total_docs']} files / {stats['total_photos']} photos)")

            count = 0
            blocked = False
            last_hb = time.time()
            last_seen = cursor
            # CONTIGUITY GUARD (2026-08-29): before appending anything, prove the
            # ledger has no gap below the cursor. Telegram only appends, so
            # copying while an earlier message is missing permanently breaks the
            # lesson order. If a gap exists we refuse the topic and report it
            # rather than making the damage worse.
            #
            # The guard now asks the panel for a single aggregate instead of
            # pulling every done_id: returning the whole ledger per claim is what
            # produced 5.1 billion D1 rows read in a day.
            gap_found = None
            try:
                g = api_post('/api/gapcheck', {'topic_root': root, 'cursor': cursor}) or {}
                gap_found = g.get('gap_at')
            except Exception as e:
                print(f'   [GUARD] contiguity probe skipped: {type(e).__name__}')
            if gap_found is not None:
                print(f'   [GUARD] ledger gap at old_msg {gap_found} (cursor={cursor}) - '
                      f'refusing to append; reporting for repair')
                api_post('/api/update', {'topic_root': root, 'cursor': cursor,
                                         'gap_at': gap_found})
                api_post('/api/release', {'topic_root': root, 'worker_id': WORKER_ID})
                continue

            try:
                it = client.iter_messages(old, reply_to=None if is_general else root,
                                          reverse=True, min_id=cursor)
                async for m in it:
                    if remaining() < 120:
                        print('[TIME] stopping mid-topic; cursor already safe')
                        await batch.flush()
                        return
                    last_seen = max(last_seen, m.id)
                    if time.time() - last_hb > 240:
                        await batch.flush()          # keep cursor durable before idle heartbeat
                        api_post('/api/heartbeat', {'topic_root': root, 'worker_id': WORKER_ID})
                        last_hb = time.time()
                    # cursor alone decides what has been processed: the panel no
                    # longer ships per-message id sets (that cost 5.1B rows read
                    # in a day). Anything at or below the cursor is done.
                    if m.id <= cursor:
                        continue
                    if is_general and topic_is_forum_topic(m):
                        continue  # belongs to a real topic - handled by its own claim
                    # HEAD-OF-LINE BLOCKING (2026-08-29).
                    # The destination only appends, so a message may NEVER be
                    # copied while an earlier one is unresolved - otherwise the
                    # topic ends up out of order and needs a tail repair.
                    # Policy: retry the same message in-run with backoff; if it
                    # still fails, STOP this topic and move to another one. The
                    # cursor is never advanced past a failure.
                    ATTEMPTS = 3
                    status = new_mid = meta = None
                    fail_item = None
                    for attempt in range(1, ATTEMPTS + 1):
                        if remaining() < 180:
                            print('[TIME] not enough budget for another attempt')
                            await batch.flush()
                            return
                        try:
                            status, new_mid, meta = await asyncio.wait_for(
                                process_message(client, new, m, new_tid, t),
                                timeout=PER_MSG_TIMEOUT)
                            fail_item = None
                            break
                        except asyncio.TimeoutError:
                            sz = getattr(getattr(m, 'document', None), 'size', 0) or 0
                            print(f'   [WATCHDOG] msg {m.id} attempt {attempt}/{ATTEMPTS} '
                                  f'exceeded {PER_MSG_TIMEOUT}s ({sz / 1048576:.0f}MB)')
                            fail_item = {'old_msg_id': m.id, 'cursor': cursor,
                                         'status': 'failed', 'kind': 'unknown',
                                         'file_name': 'watchdog-timeout',
                                         'caption': (m.message or '')[:200]}
                        except errors.FloodWaitError as e:
                            wait = min(e.seconds + 2, max(0, remaining() - 60))
                            if wait <= 0:
                                print('[TIME] floodwait beyond budget, exiting')
                                await batch.flush()
                                return
                            print(f'   [FLOODWAIT] {e.seconds}s')
                            await asyncio.sleep(wait)
                            continue           # floodwait is not a failure
                        except Exception as e:
                            print(f'   [ERR] msg {m.id} attempt {attempt}/{ATTEMPTS}: '
                                  f'{type(e).__name__}: {str(e)[:110]}')
                            fail_item = {'old_msg_id': m.id, 'cursor': cursor,
                                         'status': 'failed', 'kind': 'unknown',
                                         'file_name': '',
                                         'caption': (m.message or '')[:200]}
                            if m.document:
                                fail_item['kind'] = 'document'
                                fail_item['file_name'] = next(
                                    (getattr(a, 'file_name', '') for a in m.document.attributes
                                     if hasattr(a, 'file_name')), '')
                                fail_item['size'] = m.document.size or 0
                            # The structured record: this is what makes the
                            # pattern findable across runs. `[ERR]` alone only
                            # ever said "Request was unsuccessful N time(s)",
                            # which is Telethon's give-up message and names no
                            # cause. Here we attach the document identity, how
                            # far the transfer got, and the telethon records that
                            # preceded it.
                            #
                            # Built as one dict, not as kwargs + **doc_identity:
                            # doc_identity() also returns `size` and `file_name`,
                            # and duplicate keyword arguments raise TypeError -
                            # inside the except block, which would turn a handled
                            # download failure into a crashed topic.
                            rec = {'msg_id': m.id, 'topic': root,
                                   'title': t.get('title'), 'attempt': attempt,
                                   'of': ATTEMPTS, 'err': type(e).__name__,
                                   'detail': str(e)[:300],
                                   'peak_mb': round(
                                       getattr(_run_monitored, 'last_peak', 0)
                                       / 1048576, 2),
                                   'tl': _tl_tail()}
                            rec.update(doc_identity(m))
                            # the ledger's own view wins for these two
                            rec['file_name'] = (fail_item.get('file_name')
                                                or rec.get('file_name') or '')
                            rec['size'] = fail_item.get('size') or rec.get('size') or 0
                            diag('msg_fail', **rec)
                        if attempt < ATTEMPTS:
                            back = 15 * attempt
                            print(f'   [RETRY] msg {m.id} again in {back}s')
                            await asyncio.sleep(back)

                    if fail_item is not None:
                        # record the failure WITHOUT advancing the cursor, then
                        # abandon this topic so nothing is appended after the hole
                        batch.add(fail_item)
                        await batch.flush()
                        print(f'   [BLOCKED] msg {m.id} failed {ATTEMPTS}x - stopping this '
                              f'topic to preserve order; another topic will be claimed')
                        diag('blocked', msg_id=m.id, topic=root, title=t.get('title'),
                             cursor=cursor, file_name=fail_item.get('file_name'),
                             size=fail_item.get('size', 0),
                             prior_tries=retry_tries)
                        _summary(f'- **BLOCKED** topic `{t.get("title")}` at old_msg '
                                 f'`{m.id}` ({fail_item.get("file_name") or "?"}), '
                                 f'{retry_tries} prior attempts')
                        api_post('/api/release', {'topic_root': root, 'worker_id': WORKER_ID})
                        blocked = True
                        break

                    payload = {'old_msg_id': m.id, 'cursor': m.id, 'status': status,
                               'new_msg_id': new_mid, **meta}
                    batch.add(payload)
                    count += 1
                    if count % 25 == 0:
                        print(f'   ... {count} msgs (at id {m.id}, {remaining():.0f}s left)')
                    await batch.maybe_flush(payload)
                if blocked:
                    await batch.flush()
                    continue
                topics_done += 1
                print(f'   [TOPIC COMPLETE] {count} msgs this run')
                # AUTO TAIL-REPAIR: if any retriable failure sits BELOW the
                # cursor, delete its post-hole copies from Telegram, rewind and
                # let the next claim replay that stretch in perfect order.
                # A file keeps retrying across waves until forwarded or until it
                # has failed 3 times (then surfaced on the dashboard).
                try:
                    rep = api_post('/api/repairinfo', {'topic_root': root}).get('repair')
                    if rep:
                        ids = [i for i in (rep.get('delete_new_ids') or []) if i]
                        done_del = 0
                        for k in range(0, len(ids), 50):
                            await client.delete_messages(new, ids[k:k + 50])
                            done_del += min(50, len(ids) - k)
                        r2 = api_post('/api/applyrepair', {'topic_root': root, 'hole': rep['hole']})
                        print(f'   [REPAIR] hole at {rep["hole"]}: deleted {done_del} '
                              f'post-hole copies -> replaying ascending '
                              f'(apply={r2.get("ok")})')
                except Exception as e:
                    print(f'   [REPAIR] skipped ({type(e).__name__}: {str(e)[:90]})')
                if is_general and last_seen > cursor:
                    # reached the true end of the group chat -> close out the row
                    await batch.flush()
                    api_post('/api/update', {'topic_root': root, 'cursor': last_seen,
                                             'top_message': last_seen})
                    print(f'   [GENERAL SEALED] top_msg={last_seen}')
            finally:
                await batch.flush()
                api_post('/api/release', {'topic_root': root, 'worker_id': WORKER_ID})
        except QuotaExhausted as e:
            # Do not release, do not mark anything: without the panel we cannot
            # know what was done. Exit so the next wave resumes from the cursor.
            print(f'\n[ABORT] {e} - stopping cleanly, no state invented')
            break
        except Exception as e:
            print(f'   [TOPIC ERR] {type(e).__name__}: {e} (releasing, moving on)')
            try:
                api_post('/api/release', {'topic_root': root, 'worker_id': WORKER_ID})
            except QuotaExhausted:
                print('[ABORT] panel rate limited during release')
                break

    print(f'\n[DONE] worker {WORKER_ID} finished, {topics_done} topics fully processed')
    print(f'[CALLS] {_calls["n"]}/{CALL_BUDGET} panel calls used')
    diag('run_end', topics_done=topics_done, calls=_calls['n'],
         budget=CALL_BUDGET, secs=round(time.time() - START, 1))
    _diag_report()


def _diag_report():
    """Roll the DIAG lines of THIS run into the Actions job summary.

    The point: the pattern must be readable from the run page, without
    downloading a log zip and grepping. Groups failures by (msg_id, error) and
    shows whether the transfer ever moved a byte, which is the distinction
    between "Telegram refused" and "the transfer broke".
    """
    if not DIAG_ON or not os.path.exists(DIAG_FILE):
        return
    try:
        rows = [json.loads(l) for l in open(DIAG_FILE, encoding='utf-8') if l.strip()]
    except Exception as e:
        print(f'[DIAG] report failed: {type(e).__name__}')
        return

    fails = [r for r in rows if r['ev'] in ('dl_fail', 'msg_fail')]
    if not fails:
        _summary(f'### Migration diagnostics\n\nNo download failures this run '
                 f'({len(rows)} diag events).')
        return

    by = {}
    for r in fails:
        k = (r.get('msg_id'), r.get('err'))
        e = by.setdefault(k, {'n': 0, 'peak': 0.0, 'stages': set(),
                              'file': r.get('file_name') or '', 'dc': r.get('dc'),
                              'size': r.get('size') or 0, 'detail': r.get('detail', '')})
        e['n'] += 1
        e['peak'] = max(e['peak'], r.get('peak_mb') or 0)
        if r.get('stage'):
            e['stages'].add(r['stage'])

    lines = ['### Migration diagnostics', '',
             '| old_msg | file | MB | DC | err | tries | max bytes seen | verdict |',
             '|---|---|---|---|---|---|---|---|']
    for (mid, err), e in sorted(by.items(), key=lambda kv: -kv[1]['n']):
        # peak == 0 across every strategy means Telegram never handed us a
        # single byte -> the file is unreadable at source, not a slow transfer.
        verdict = 'never sent a byte (source-side)' if e['peak'] == 0 \
            else f'broke mid-transfer at {e["peak"]}MB'
        lines.append(
            f'| `{mid}` | {(e["file"] or "?")[:44]} | '
            f'{round((e["size"] or 0) / 1048576, 1)} | {e["dc"]} | `{err}` | '
            f'{e["n"]} | {e["peak"]}MB | {verdict} |')

    refetch = [r for r in rows if r['ev'] == 'refetch_ok']
    if refetch:
        lines += ['', f'**{len(refetch)} file(s) succeeded only after re-fetching '
                      f'the message** — those were STALE file_reference, not dead '
                      f'files: ' + ', '.join(f'`{r["msg_id"]}`' for r in refetch)]

    gone = [r for r in rows if r['ev'] == 'refetch_gone']
    if gone:
        lines += ['', f'**{len(gone)} message(s) no longer visible at source:** '
                  + ', '.join(f'`{r["msg_id"]}`' for r in gone)]

    lines += ['', '<details><summary>telethon tail for the first failure</summary>',
              '', '```']
    lines += [str(x) for x in (fails[0].get('tl') or ['(none captured)'])]
    lines += ['```', '</details>']
    if _summary('\n'.join(lines)):
        print('[DIAG] job summary written')


def StringSessionHolder():
    from telethon.sessions import StringSession
    return StringSession(SESSION)


try:
    asyncio.run(main())
except QuotaExhausted as e:
    # A clean, non-failing exit: the wave simply could not run. Marking the job
    # failed would be misleading - nothing is broken and no data is at risk.
    print(f'[QUOTA] {e}')
    print('[QUOTA] exiting 0 - next wave will resume from the saved cursor')
    diag('quota_abort', detail=str(e)[:200])
    _summary(f'### Migration diagnostics\n\n**Aborted on account-wide rate '
             f'limit** — no state invented, next wave resumes from the cursor.')
except BaseException as e:
    # Any other exit path must still leave the diagnostics behind, otherwise a
    # crash is exactly the case where we learn nothing.
    diag('crash', err=type(e).__name__, detail=str(e)[:300], tl=_tl_tail())
    _diag_report()
    raise


