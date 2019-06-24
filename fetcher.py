import sqlite3
from concurrent.futures import ThreadPoolExecutor
from threading import Thread
from queue import Queue, Empty
import time
import json
import signal
import subprocess
import glob
import os
from collections import namedtuple

from logger import ThreadsafeLogger
from utils import yt_dl, compress_json

Job = namedtuple('Job', 'cv_id v_id retry')

class VideoFetcher:

    def __init__(self, db):
        self.db = db

    def load_settings(self, workers, bandwidth, max_retry):
        self.workers = workers
        # In Mb/s from the config
        self.bandwidth = bandwidth * 1024 * 1024
        self.rate_limit = self.bandwidth / self.workers / 8
        self.max_retry = max_retry

    def run(self):
        self.running = True
        self.pool = ThreadPoolExecutor(max_workers=self.workers)
        self.logger = ThreadsafeLogger()
        self.jobthread = Thread(target=self.try_main_loop)
        self.child_processes = {}
        self.futures = {}
        self.queued_videos = {}
        self.done_queue = Queue()
        self.logger.start()
        self.jobthread.start()

    def stop(self):
        self.log("Stopping")
        self.running = False
        self.jobthread.join()
        self.log("Halt logging thread")
        self.logger.stop()
        self.log("Stopped")
        self.logger.drain_logqueue()

    def log(self, fmt, *args):
        self.logger.log(fmt % args)

    def re_queue(self, result, c):
        c.execute('INSERT OR IGNORE INTO fetch_jobs (cv_id, retry) VALUES (?, ?)',
                  (result['job'].cv_id, result['job'].retry + 1))

    def post_process(self, result, c):
        job = result['job']
        del self.queued_videos[job.v_id]
        if result['error']:
            self.re_queue(result, c)
        else:
            data = result['data']
            self.check_metadata(c, job, data)
            fn = result['filename']
            c.execute('INSERT INTO stored_video (cv_id, video_filename) VALUES (?, ?)', (
                job.cv_id, fn))
            store_id = c.lastrowid
            raw = compress_json(data)
            c.execute('INSERT INTO video_raw_meta (store_id, compressed_json) VALUES (?, ?)', (store_id, raw))
            basename = fn[:fn.rindex('.') + 1]
            files = glob.glob(glob.escape(basename) + '*')
            files.remove(fn)
            for f in files:
                if f.endswith('.jpg'):
                    c.execute('INSERT INTO thumbnail_file (store_id, filename) VALUES (?, ?)', (store_id, f))
                elif f.endswith('.vtt'):
                    enddot = f.rindex('.')
                    startdot = f[:enddot].rindex('.')
                    lang = f[startdot + 1:enddot]
                    c.execute('INSERT INTO subtitle_files (store_id, language, filename) VALUES (?, ?, ?)', (store_id, lang, f))
                else:
                    self.log('[%s] Error: Unknown file %s', job.v_id, f)

    def check_metadata(self, c, job, data):
        def check_eq(val, key):
            if val != data[key]:
                self.log('[%s] Different %s: %r != %r', job.v_id, key, val, data[key])
        row = c.execute('SELECT title, description, duration FROM channel_video WHERE id = ?',
                  (job.cv_id,)).fetchone()
        check_eq(row[0], 'fulltitle')
        check_eq(row[1], 'description')
        check_eq(row[2], 'duration')
        row = c.execute('SELECT c.channel_id, c.title, c.username FROM channels c JOIN channel_video cv ON cv.channel_id = c.id WHERE cv.id = ?',
                        (job.cv_id,)).fetchone()
        check_eq(row[0], 'channel_id')
        check_eq(row[1], 'uploader')
        check_eq(row[2], 'uploader_id')

    def try_main_loop(self):
        try:
            self.main_loop()
        finally:
            # Stopped running
            self.log("Job thread stopping")
            self.log("Cancelling all pending tasks")
            for f in self.futures.values():
                f.cancel()

            self.log("Closing all youtube-dl processes")
            for p in self.child_processes.values():
                p.send_signal(signal.SIGINT)
            self.log("Waiting for child termination")
            for p in list(self.child_processes.values()):
                p.wait()

            self.log("Shutdown threadpool")
            self.pool.shutdown()

            self.log("Processing finished jobs")
            c = self.db.cursor()
            self.process_done_queue(c)

            self.child_processes = {}
            self.futures = {}

            if self.queued_videos:
                self.log("Requeuing unfinished jobs")
                for job in self.queued_videos.values():
                    c.execute('INSERT OR IGNORE INTO fetch_jobs (cv_id, retry) VALUES (?, ?)',
                      (job.cv_id, job.retry))
                self.queued_videos = {}

            self.db.commit()
            

    def main_loop(self):
        self.log("Begin polling")
        retry_commit = False
        Q = 'SELECT v.id, v.video_id, retry FROM fetch_jobs JOIN channel_video v ON v.id = cv_id WHERE retry < ?'
        while self.running:
            c = self.db.cursor()
            need_commit = retry_commit
            rows = c.execute(Q, (self.max_retry,)).fetchall()
            if rows:
                self.log('Submitting %d jobs', len(rows))
                for cv_id, v_id, retry in rows:
                    need_commit = True
                    c.execute('DELETE FROM fetch_jobs WHERE cv_id = ?',
                              (cv_id,))
                    if v_id in self.queued_videos:
                        self.log('[%s] Refusing to enqueue, already in queue', v_id)
                        continue
                    exists = c.execute('SELECT 1 FROM stored_video WHERE cv_id = ?', (
                        cv_id,)).fetchone()
                    if exists:
                        self.log('[%s] Refusing to download, already exists', v_id)
                    else:
                        self.add_job(Job(cv_id, v_id, retry))

            if self.process_done_queue(c):
                needs_commit = True

            if needs_commit:
                try:
                    self.db.commit()
                    retry_commit = False
                except:
                    retry_commit = True
                    self.logerror('JobThread')
            time.sleep(0.5)

    def add_job(self, job):
        f_id = len(self.futures)
        self.queued_videos[job.v_id] = job
        future = self.pool.submit(self.try_download_video, f_id, job)
        self.futures[f_id] = future

    def process_done_queue(self, c):
        need_commit = False
        while True:
            try:
                result = self.done_queue.get(False)
            except Empty:
                break
            need_commit = True
            try:
                self.post_process(result, c)
            except:
                self.logerror(result['job'].v_id)
                self.re_queue(result, c)
        return need_commit

    def logerror(self, v_id):
        self.log("[%s] Python exception", v_id)
        import traceback
        for line in traceback.format_exc().splitlines():
            self.log("[%s] %s", v_id, line)

    def try_download_video(self, f_id, job):
        if not self.jobthread.is_alive():
            self.log("[%s] Job thread died", job.v_id)
            del self.futures[f_id]
            return
        try:
            self.download_video(f_id, job)
        except:
            self.logerror(job.v_id)
            if f_id in self.futures:
                del self.futures[f_id]
            self.done_queue.put({'job': job, 'error': True})

    def download_video(self, f_id, job):
        v_id = job.v_id
        self.log('[%s] Begin download. Attempt %d', v_id, retry)
        outfmt = r'Videos/%(uploader)s/%(upload_date)s - %(title)s - %(id)s.%(ext)s'
        fmt = 'bestvideo[height <=? 1080]+bestaudio/best[height <=? 1080]/best'
        url = 'https://www.youtube.com/watch?v=%s' % v_id

        p = subprocess.Popen(yt_dl([
                '--print-json',
                '--limit-rate', str(self.rate_limit),
                '--no-progress',
                '--output', outfmt,
                '--format', fmt,
                '--ignore-errors',
                '--no-continue',
                '--no-overwrites',
                '--no-post-overwrites',
                '--write-thumbnail',
                '--write-sub',
                '--all-subs',
                url
            ]), stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                universal_newlines=True)
        self.log('[%s] Spawned process PID %d', v_id, p.pid)
        self.child_processes[p.pid] = p
        outs, errs = p.communicate()
        del self.child_processes[p.pid]

        for line in errs.splitlines():
            self.log('[%s] youtube-dl stderr: %s', v_id, line.strip())
        result = {'job': job}
        if p.returncode == 0:
            result['error'] = False
            ret = json.loads(outs)
            fn = ret['_filename']
            if '+' in ret['format_id']:
                # This is a combined file in mkv
                fn = fn[:fn.rindex('.')] + '.mkv'
            del ret['_filename']
            result['data'] = ret
            result['filename'] = fn
            if not os.path.isfile(fn):
                raise Exception('File not found: %s' % fn)
            self.log('[%s] Success. File: %s', v_id, fn)
        else:
            result['error'] = True
            self.log('[%s] Error. Return code %d', v_id, p.returncode)

        del self.futures[f_id]
        self.done_queue.put(result)


if __name__ == '__main__':
    with open('config.json', 'r') as f:
        config = json.load(f)
    db = sqlite3.connect(config['database']['file'], check_same_thread=False)
    db.execute('PRAGMA foreign_keys = ON')
    f_config = config['video_fetcher']
    f = VideoFetcher(db)
    f.load_settings(f_config['workers'], f_config['bandwidth'],
                    f_config['max_retry'])
    def signal_stop(sig, stack):
        f.stop()
        db.close()
    signal.signal(signal.SIGINT, signal_stop)
    signal.signal(signal.SIGTERM, signal_stop)
    f.run()
