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
SESSION = os.environ['TELEGRAM_SESSION']
OLD_ID = int(os.environ['OLD_CHAT_ID'])
NEW_ID = int(os.environ['NEW_CHAT_ID'])
WORKER = os.environ['WORKER_URL'].rstrip('/')
KEY = os.environ['MIGRATE_KEY']
BUDGET_MIN = float(os.environ.get('TIME_BUDGET_MIN', '48'))
SHARD = os.environ.get('SHARD', 'manual')
WORKER_ID = 'shard-' + SHARD + '-' + str(os.getpid())
DL_DIR = '/tmp/migdl'
START = time.time()

HDRS = {'x-migrate-key': KEY, 'content-type': 'application/json'}


def api_get(path):
    r = requests.get(WORKER + path, headers=HDRS, timeout=30)
    r.raise_for_status()
    return r.json()


def api_post(path, payload):
    for attempt in range(4):
        try:
            r = requests.post(WORKER + path, headers=HDRS, data=json.dumps(payload), timeout=30)
            if r.status_code == 200:
                return r.json()
        except Exception as e:
            print(f'   worker post retry {attempt}: {e}')
        time.sleep(3 * (attempt + 1))
    print('   [WARN] worker unreachable, continuing (state may replay)')
    return {}


def remaining():
    return BUDGET_MIN * 60 - (time.time() - START)


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
        path = await client.download_media(m, file=os.path.join(DL_DIR, safe_fn))
        if path:
            try:
                vid = is_video(doc)
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


async def main():
    client = TelegramClient(StringSessionHolder(), API_ID, API_HASH)
    await client.connect()
    if not await client.is_user_authorized():
        print('[FATAL] session invalid')
        sys.exit(1)
    me = await client.get_me()
    old = await client.get_entity(OLD_ID)
    new = await client.get_entity(NEW_ID)
    print(f'[OK] {me.first_name} | worker={WORKER_ID} | budget {BUDGET_MIN} min')

    st = api_get('/api/state')
    if not st.get('topics'):
        await seed_topics(client, old)

    topics_done = 0
    while remaining() > 300:
        c = api_post('/api/claim', {'worker_id': WORKER_ID})
        t = c.get('topic')
        if not t:
            print('[CLAIM] nothing left to migrate - all caught up')
            break
        root = t['old_root']
        cursor = t.get('cursor') or 0
        done_ids = set(c.get('done_ids') or [])
        print(f"\n=== CLAIMED {t['title']!r} (root={root}, cursor={cursor}, done_ids={len(done_ids)}) ===")

        try:
            new_tid = await ensure_new_topic(client, new, t)
            if not new_tid:
                print('   [WARN] no new topic id, releasing')
                continue

            if t.get('total_msgs') is None:
                if remaining() < 600:
                    print('[TIME] not enough budget to scan a new topic, releasing')
                    continue
                total, media = await count_totals(client, old, root)
                api_post('/api/totals', {'topic_root': root, 'total_msgs': total, 'total_media': media})
                print(f'   [COUNT] {total} copyable msgs, {media} media')

            count = 0
            last_hb = time.time()
            try:
                it = client.iter_messages(old, reply_to=root, reverse=True, min_id=cursor)
                async for m in it:
                    if remaining() < 120:
                        print('[TIME] stopping mid-topic; cursor already safe')
                        return
                    if time.time() - last_hb > 240:
                        api_post('/api/heartbeat', {'topic_root': root, 'worker_id': WORKER_ID})
                        last_hb = time.time()
                    if m.id <= cursor or m.id in done_ids:
                        continue
                    try:
                        status, new_mid, meta = await process_message(client, new, m, new_tid, t)
                    except errors.FloodWaitError as e:
                        wait = min(e.seconds + 2, max(0, remaining() - 60))
                        if wait <= 0:
                            print('[TIME] floodwait beyond budget, exiting')
                            return
                        print(f'   [FLOODWAIT] {e.seconds}s')
                        await asyncio.sleep(wait)
                        continue
                    except Exception as e:
                        print(f'   [ERR] msg {m.id}: {type(e).__name__}: {str(e)[:120]}')
                        fmeta = {'topic_root': root, 'cursor': m.id, 'old_msg_id': m.id,
                                 'status': 'failed', 'kind': 'unknown', 'file_name': '',
                                 'caption': (m.message or '')[:200]}
                        if m.document:
                            fmeta['kind'] = 'document'
                            fmeta['file_name'] = next((getattr(a, 'file_name', '') for a in m.document.attributes
                                                       if hasattr(a, 'file_name')), '')
                            fmeta['size'] = m.document.size or 0
                        api_post('/api/update', fmeta)
                        continue

                    payload = {'topic_root': root, 'cursor': m.id, 'status': status,
                               'new_msg_id': new_mid, **meta}
                    api_post('/api/update', payload)
                    count += 1
                    if count % 10 == 0:
                        print(f'   ... {count} msgs (at id {m.id}, {remaining():.0f}s left)')
                topics_done += 1
                print(f'   [TOPIC COMPLETE] {count} msgs this run')
            finally:
                api_post('/api/release', {'topic_root': root, 'worker_id': WORKER_ID})
        except Exception as e:
            print(f'   [TOPIC ERR] {type(e).__name__}: {e} (releasing, moving on)')
            api_post('/api/release', {'topic_root': root, 'worker_id': WORKER_ID})

    print(f'\n[DONE] worker {WORKER_ID} finished, {topics_done} topics fully processed')


def StringSessionHolder():
    from telethon.sessions import StringSession
    return StringSession(SESSION)


asyncio.run(main())


def StringSessionHolder():
    from telethon.sessions import StringSession
    return StringSession(SESSION)


asyncio.run(main())

