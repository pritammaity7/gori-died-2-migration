"""Gori Died 2 -> My Courses migration worker.

Runs in GitHub Action (max ~48 min budget). Per-topic cursor state lives in the
Cloudflare panel worker's D1 database, updated after EVERY message, so a crash
never duplicates or skips anything. Order: oldest -> newest per topic.

Env: TELEGRAM_API_ID, TELEGRAM_API_HASH, TELEGRAM_SESSION (StringSession),
     OLD_CHAT_ID, NEW_CHAT_ID, WORKER_URL, MIGRATE_KEY, TIME_BUDGET_MIN
"""
import asyncio, os, sys, time, json
import requests
from telethon import TelegramClient, errors
from telethon.tl.functions.messages import GetForumTopicsRequest, CreateForumTopicRequest

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
# serve the bytes to anyone. Proven on 2026-08-29: one PDF failed from all five
# user identities with fresh file_references, and sendMedia re-send is blocked by
# the source group's forward restriction, so no legal path exists. A dead message
# gets a placeholder in its exact position, which keeps the sequence intact and
# means we never have to delete-and-replay a topic again.
DEAD_AFTER = int(os.environ.get('DEAD_AFTER', '24'))

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


class QuotaExhausted(RuntimeError):
    """Panel is rate limited account-wide; stop without inventing failures."""


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
            if r.status_code == 429:
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
                time.sleep(20)
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
                f'panel rate limited on {_calls["rate_limited"]} consecutive calls')
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
    Steady large transfers are unaffected regardless of duration."""
    task = asyncio.create_task(coro)
    last, last_chg = -1, time.time()
    try:
        while True:
            done, _ = await asyncio.wait({task}, timeout=20)
            if task in done:
                return task.result()
            try:
                cur = os.path.getsize(path)
            except OSError:
                cur = 0
            if cur != last:
                last, last_chg = cur, time.time()
            elif time.time() - last_chg > STALL_SECS:
                task.cancel()
                try:
                    await task
                except Exception:
                    pass
                raise RuntimeError(f'{label}: stalled {STALL_SECS:.0f}s '
                                   f'(zero progress at {cur / 1048576:.0f}MB)')
    except BaseException:
        if not task.done():
            task.cancel()
        raise


async def smart_download(client, msg, path):
    """Download with three escalating strategies.

    Root cause of the repeated failures (from the Action logs, 2026-08-29):
        Telegram is having internal issues TimeoutError: Timeout while fetching
        data (caused by GetFileRequest)   ->  ValueError: Request was unsuccessful
        6 time(s)
    Telethon retries a GetFileRequest `request_retries` times (default 5, hence
    "6 time(s)") and then gives up. Per Telethon's docs those retries fire on
    ServerError / RpcCallFail / migrate errors - i.e. a *transient* DC problem.
    The file is fine; the DC route is briefly unhealthy.

    So instead of failing the message we escalate:
      1. FastTelethon parallel download (many senders, fastest)
      2. standard download with a smaller chunk size (gentler on a sick DC)
      3. standard download after a cooldown, giving the DC time to recover
    Only if all three fail does the caller record a failure - and even then the
    message is never skipped, it just blocks its own topic until it succeeds.
    """
    doc = getattr(msg, 'document', None)
    size = (doc.size if doc else 0) or 0

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

        try:
            return await _run_monitored(f'STD-DL/{part // 1024}k', run_std(), path)
        except Exception as e:
            print(f'   [STD-DL attempt {attempt}] {type(e).__name__}: {str(e)[:90]}')
            try:
                os.remove(path)
            except OSError:
                pass
            if attempt == 2:
                raise


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
        if size > 1950 * 1024 * 1024:
            return 'skipped', None, meta  # over Telegram 2GB upload cap
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
            if retry_tries >= DEAD_AFTER:
                # Every retry path has been exhausted many times over. Post a
                # placeholder IN POSITION so the sequence keeps its slot, record
                # the message as dead, and let the topic continue. No deletion,
                # no replay - the position is occupied, so there is no hole.
                info = api_post('/api/deadinfo', {'topic_root': root,
                                                  'old_msg_id': retry_from}) or {}
                name = info.get('file_name') or ''
                try:
                    ph = await client.send_message(
                        new, f'⚠️ Missing file: {name or f"message {retry_from}"}\n'
                             f'Telegram no longer serves this file from the source '
                             f'channel, so it could not be copied. Everything before '
                             f'and after it is complete and in order.',
                        reply_to=await ensure_new_topic(client, new, t))
                    api_post('/api/dead', {
                        'topic_root': root, 'old_msg_id': retry_from,
                        'new_msg_id': ph.id, 'file_name': name,
                        'size': info.get('size') or 0, 'kind': info.get('kind') or 'unknown',
                        'reason': 'unreadable at source after %d attempts' % retry_tries})
                    print(f'   [DEAD] old_msg {retry_from} marked dead; placeholder '
                          f'{ph.id} posted in position')
                    continue
                except Exception as e:
                    print(f'   [DEAD] placeholder failed: {type(e).__name__}: {str(e)[:80]}')
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


