import subprocess
import json
from queue import Queue, Empty
from concurrent.futures import ThreadPoolExecutor
from threading import Thread
import time
import sqlite3
import signal

from logger import ThreadsafeLogger
from utils import yt_dl

class MetadataFetcher:

    def __init__(self, db):
        self.db = db
        self.workers = 8 # TODO: Hardcoded for now
        self.max_retry = 3

    def store_channel(self, c, channel_id, name, username):
        exists = c.execute('SELECT id FROM channel WHERE channel_id = ?',
                           (channel_id,)).fetchone()
        if exists is not None:
            return exists[0]

        c.execute('INSERT INTO channel (channel_id, name, username) VALUES (?, ?, ?)', (
            channel_id, name, username))
        return c.lastrowid
        
    def store_video(self, c, channel_id, video_id, date, title, description,
                    duration):
        c.execute('INSERT INTO channel_video (channel_id, video_id, upload_date, title, description, duration) VALUES (?, ?, ?, ?, ?, ?)', (
            channel_id, video_id, date, title, description, duration))
        return c.lastrowid

    def store_from_yt_dl(self, c, data):
        channel_id = self.store_channel(c, data['channel_id'], data['uploader'],
                                        data['uploader_id'])
        title = data['fulltitle'] if 'fulltitle' in data else data['title']
        return self.store_video(c, channel_id, data['id'], data['upload_date'],
                                title, data['description'],
                                data['duration'])

    def store_blocked_video(self, c, video_id, channel_id):
        c.execute('INSERT INTO blocked_video (video_id, channel_id) VALUES (?, ?)', (
            video_id, channel_id))

    def run(self):
        self.running = True
        self.video_queue = Queue()
        self.video_fetch_queue = Queue()
        self.retry_queue = Queue()
        self.pool = ThreadPoolExecutor(max_workers=self.workers)
        self.logger = ThreadsafeLogger()
        self.jobthread = Thread(target=self.poll_jobs)
        self.child_processes = {}
        self.futures = {}

        self.logger.start()
        self.jobthread.start()

    def stop(self):
        self.log("Stopping")
        for f in self.futures.values():
            f.cancel()
        self.running = False
        self.log("Closing all youtube-dl processes")
        for p in self.child_processes.values():
            p.send_signal(signal.SIGINT)
        self.log("Waiting for child termination")
        for p in list(self.child_processes.values()):
            p.wait()
        self.log("Shutdown threadpool")
        self.pool.shutdown()
        self.log("Halt logging thread")
        self.logger.stop()
        self.log("Halt job queue thread")
        self.jobthread.join()
        self.log("Stopped")
        self.logger.drain_logqueue()
        self.child_processes = {}
        self.futures = {}

    def log(self, fmt, *args):
        self.logger.log(fmt % args)

    def poll_jobs(self):
        self.log("Begin polling")
        retry_commit = False
        while self.running:
            c = self.db.cursor()
            needs_commit = retry_commit
            channels = c.execute('SELECT channel_id, retry FROM channel_fetch_jobs WHERE retry < ?', (
                self.max_retry,)) \
                       .fetchall()
            if channels:
                self.log("Fetching %d channels", len(channels))
            for channel, retry in channels:
                needs_commit = True
                c.execute('DELETE FROM channel_fetch_jobs WHERE channel_id = ?',
                          (channel,))
                if channel in self.futures:
                    self.log("[%s] Refusing to enqueue, already in queue", channel)
                    continue
                exists = c.execute('SELECT id FROM channel WHERE channel_id = ?',
                                   (channel,)).fetchone()
                blocked = c.execute('SELECT video_id FROM blocked_video WHERE channel_id = ?', (
                    channel,)).fetchall()
                existing = set()
                if exists is not None:
                    exist_db = c.execute('SELECT video_id FROM channel_video WHERE channel_id = ?', (
                        exists[0],)).fetchall()
                    for v_id, in exist_db:
                        existing.add(v_id)
                if blocked is not None:
                    for v_id, in blocked:
                        existing.add(v_id)
                self.add_channel_fetch_job(channel, existing)

            if self.store_all_queued(c):
                needs_commit = True

            if self.add_all_retry(c):
                needs_commit = True

            while True:
                try:
                    ch_id, v_id = self.video_fetch_queue.get(False)
                except Empty:
                    break
                self.add_video_fetch_job(ch_id, v_id)

            if needs_commit:
                try:
                    self.db.commit()
                    retry_commit = False
                except:
                    retry_commit = True
                    self.logerror('JobThread')
            time.sleep(0.5)

        # Stopped running
        self.log("Job thread stopping. Flushing queues")
        c = self.db.cursor()
        self.store_all_queued(c)
        self.add_all_retry(c)
        for channel_id in self.futures.keys():
            c.execute('INSERT OR IGNORE INTO channel_fetch_jobs (channel_id) VALUES (?)', (
                channel_id,))
        self.db.commit()

    def store_all_queued(self, c):
        count = 0
        while True:
            try:
                vid_meta = self.video_queue.get(False)
            except Empty:
                break
            try:
                if '##error' in vid_meta:
                    self.store_blocked_video(c, vid_meta['id'], vid_meta['channel_id'])
                    count += 1
                    continue
                self.store_from_yt_dl(c, vid_meta)
                count += 1
            except:
                self.logerror(vid_meta['id'])
                self.retry_queue.put(vid_meta['channel_id'])
        if count > 0:
            self.log("Saved metadata of %d videos", count)
        return count > 0

    def add_all_retry(self, c):
        needs_commit = False
        while True:
            try:
                channel_id = self.retry_queue.get(False)
            except Empty:
                break
            needs_commit = True
            # TODO implement retry counter
            c.execute('INSERT OR IGNORE INTO channel_fetch_jobs (channel_id, retry) VALUES (?, ?)', (
                channel_id, self.max_retry))

    def add_channel_fetch_job(self, yt_channel_id, existing):
        future = self.pool.submit(self.try_fetch_channel, yt_channel_id, existing)
        self.futures[yt_channel_id] = future

    def add_video_fetch_job(self, channel_id, video_id):
        self.pool.submit(self.try_fetch_video_meta, channel_id, video_id)

    def logerror(self, key):
        self.log("[%s] Python exception", key)
        import traceback
        for line in traceback.format_exc().splitlines():
            self.log("[%s] %s", key, line)

    def try_fetch_channel(self, yt_channel_id, existing):
        if not self.jobthread.is_alive():
            self.log("[%s] Job thread died", yt_channel_id)
            del self.futures[yt_channel_id]
            return
        try:
            self.fetch_channel(yt_channel_id, existing)
        except:
            self.logerror(yt_channel_id)
            self.retry_queue.put(yt_channel_id)
        del self.futures[yt_channel_id]

    def fetch_channel(self, yt_channel_id, existing):
        self.log("[%s] Begin fetch", yt_channel_id)
        if not existing:
            self.bulk_fetch_channel(yt_channel_id)
        else:
            self.partial_fetch_channel(yt_channel_id, existing)

    def bulk_fetch_channel(self, yt_channel_id):
        url = 'https://www.youtube.com/channel/%s' % yt_channel_id
        out, er, rc = self._popen(yt_channel_id, yt_dl(['-J', url]))
        if rc != 0:
            self.log("[%s] Non-zero return: %d.", yt_channel_id, rc)
            if self.running:
                self.log("[%s] Switch to partial fetch", yt_channel_id)
                self.partial_fetch_channel(yt_channel_id, set())
            else:
                self.retry_queue.put(yt_channel_id)
            return
        playlist = json.loads(out)
        for entry in playlist['entries']:
            self.video_queue.put(entry)
        self.log("[%s] Bulk imported %d entries", yt_channel_id, len(playlist['entries']))

    def partial_fetch_channel(self, yt_channel_id, existing):
        url = 'https://www.youtube.com/channel/%s' % yt_channel_id
        out, er, rc = self._popen(yt_channel_id, yt_dl(['-J', '--flat-playlist', url]))
        if rc != 0:
            self.log("[%s] Non-zero return: %d", yt_channel_id, rc)
            self.retry_queue.put(yt_channel_id)
            return
        playlist = json.loads(out)
        count = 0
        for entry in playlist['entries']:
            if entry['id'] not in existing:
                count += 1
                self.video_fetch_queue.put((yt_channel_id, entry['id']))
        self.log("[%s] Queued %d videos to fetch metadata", yt_channel_id, count)

    def try_fetch_video_meta(self, channel_id, video_id):
        if not self.jobthread.is_alive():
            self.log("[%s] Job thread died", video_id)
            return
        try:
            self.fetch_video_meta(channel_id, video_id)
        except:
            self.logerror(video_id)
            self.retry_queue.put(channel_id)

    def fetch_video_meta(self, channel_id, video_id):
        key = channel_id + ':' + video_id
        url = 'https://www.youtube.com/watch?v=%s' % video_id
        out, er, rc = self._popen(key, yt_dl(['-j', url]))
        if rc != 0:
            self.log("[%s] Non-zero return: %d", key, rc)
            if er.find('blocked') != -1 \
               or er.find('video is unavailable') != -1 \
               or er.find('video is not available') != -1 \
               or er.find('not made this video available') != -1 \
               or er.find('It is not available') != -1 \
               or er.find('Playback on other websites has been disabled') != -1:
                self.video_queue.put({'##error': True, 'id': video_id,
                                      'channel_id': channel_id})
                return
            self.retry_queue.put(channel_id)
            return
        meta = json.loads(out)
        self.video_queue.put(meta)

    def _popen(self, key, args):
        p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                             universal_newlines=True)
        self.log("[%s] Spawned process PID %d", key, p.pid)
        self.child_processes[p.pid] = p
        outs, errs = p.communicate()
        del self.child_processes[p.pid]
        for line in errs.splitlines():
            self.log("[%s] stderr: %s", key, line.strip())
        return outs, errs, p.returncode

if __name__ == '__main__':
    db = sqlite3.connect('youtube.db', check_same_thread=False)
    f = MetadataFetcher(db)
    def interrupt(sig, stack):
        f.stop()
        db.close()
    signal.signal(signal.SIGINT, interrupt)
    f.run()
