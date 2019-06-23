import subprocess
import json
from queue import Queue, Empty
from concurrent.futures import ThreadPoolExecutor
from threading import Thread
import time
import sqlite3
import signal
from collections import namedtuple

from logger import ThreadsafeLogger
from utils import yt_dl, compress_json, iso8601_duration_as_seconds
from youtube_api import YoutubeAPI, APIKeyAuthMode

ChannelDataFetchJob = namedtuple('ChannelDataFetchJob', 'key retry')
ChannelVideosFetchJob = namedtuple('ChannelVideosFetchJob', 'key retry ch_id aux_data')
VideoDataFetchJob = namedtuple('VideoDataFetchJob', 'key cv_id ch_id retry')

Result = namedtuple('Result', 'value job error')

class Bucket:

    def __init__(self, capacity, time_limit, drain):
        self.max = capacity
        self.limit = time_limit
        self.ttl = time_limit
        self.items = []
        self.drain = drain

    def add(self, item):
        self.items.append(item)
        if len(self.items) == self.max:
            self.flush()

    def flush(self):
        items = self.items
        if not items:
            return
        self.items = []
        self.drain(items)

    def tick(self, seconds):
        self.ttl -= seconds
        if self.ttl <= 0:
            self.ttl = self.limit
            self.flush()

class Task:

    def __init__(self, fetcher, action, capacity, time_limit):
        self.bucket = Bucket(capacity, time_limit, self._drain)
        self.action = action
        self.fetcher = fetcher
        self.result_queue = Queue()

    def add(self, job):
        self.bucket.add(job)

    def process(self, time_passed):
        self.bucket.tick(time_passed)
        return self.drain_queue()

    def drain_queue(self):
        while True:
            try:
                yield self.result_queue.get(False)
            except Empty:
                break

    def _drain(self, items):
        # TODO remove future once done
        future = self.fetcher.pool.submit(self._do_process, items)
        self.fetcher.futures.append(future)

    def _do_process(self, jobs):
        to_process = { job.key: job for job in jobs }
        jobs = list(to_process.values())
        most_recent_job = None
        try:
            for data, job in self.action(jobs):
                most_recent_job = job
                self.result_queue.put(Result(data, job, False))
                if job.key in to_process:
                    del to_process[job.key]
        except:
            self.fetcher.logerror(most_recent_job.key if most_recent_job else 'Task')
            # Re-add job that crashed so we can report its error
            if most_recent_job is not None:
                to_process[most_recent_job.key] = most_recent_job
        # for all that remain, report error
        for job in to_process.values():
            self.result_queue.put(Result(None, job, True))

class MetadataFetcher:

    def __init__(self, db, workers, max_retry, yt_api_auth):
        self.db = db
        self.workers = workers
        self.max_retry = max_retry
        self.backend = YoutubeAPIBackend(YoutubeAPI(yt_api_auth))

    def run(self):
        self.running = True
        self.pool = ThreadPoolExecutor(max_workers=self.workers)
        self.logger = ThreadsafeLogger()
        self.jobthread = Thread(target=self.try_main_loop)
        self.futures = []
        self.active_channels = set()
        self.active_videos = set()
        self.cdata_fetch_task = Task(self, self.channel_data_fetch, 50, 10)
        self.cvideo_fetch_task = Task(self, self.channel_video_fetch, 1, 0)
        self.video_fetch_task = Task(self, self.video_data_fetch, 50, 10)
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

    def logerror(self, key):
        self.log("[%s] Python exception", key)
        import traceback
        for line in traceback.format_exc().splitlines():
            self.log("[%s] %s", key, line)

    def try_main_loop(self):
        try:
            self.main_loop()
        finally:
            # Stopped running
            self.log("Job thread stopping")
            self.log("Cancelling all pending tasks")
            for f in self.futures:
                f.cancel()
            self.futures = []
            self.log("Shutting down threadpool")
            self.pool.shutdown()
            self.log("Requeuing unfinished jobs")
            c = self.db.cursor()
            for channel_id in self.active_channels:
                # -1 because retry count is incremented
                self.retry_channel_fetch(c, channel_id, -1)
            self.active_channels = set()
            for cv_id in self.active_videos:
                self.retry_video_fetch(c, cv_id, -1)
            self.active_videos = set()
            self.db.commit()

    def pop_from_db_queue(self, c, id_field, table):
        items = c.execute('SELECT %s, retry FROM %s WHERE retry < ?' % (id_field, table),
                          (self.max_retry,)).fetchall()
        if items:
            self.log("Popping %d items from %s", len(items), table)

        for key, retry in items:
            c.execute('DELETE FROM %s WHERE %s = ?' % (table, id_field), (key,))
            yield key, retry

    def main_loop(self):
        self.log("Begin polling")
        retry_commit = False
        sleep = 0.5
        while self.running:
            c = self.db.cursor()
            needs_commit = retry_commit
            for channel_id, retry in self.pop_from_db_queue(c, 'channel_id',
                                                            'channel_fetch_jobs'):
                needs_commit = True
                if channel_id in self.active_channels:
                    self.log("[%s] Refusing to enqueue, already processing", channel_id)
                    continue
                self.active_channels.add(channel_id)

                exists = c.execute('SELECT id FROM channels WHERE channel_id = ?',
                                   (channel_id,)).fetchone()
                if exists is not None:
                    self.add_channel_video_fetch(c, exists[0], channel_id, retry)
                else:
                    self.cdata_fetch_task.add(ChannelDataFetchJob(channel_id, retry))

            for cv_id, retry in self.pop_from_db_queue(c, 'cv_id',
                                                       'video_meta_fetch_jobs'):
                needs_commit = True
                if cv_id in self.active_videos:
                    self.log("[cv %d] Refusing to enqueue, already processing", cv_id)
                    continue
                ch_id, video_id = c.execute('SELECT ch_id, video_id \
                                             FROM channel_video WHERE id = ?',
                          (cv_id,)).fetchone()
                self.active_videos.add(cv_id)
                self.video_fetch_task.add(VideoDataFetchJob(video_id, cv_id, ch_id, retry))
                

            for data, job, error in self.cdata_fetch_task.process(sleep):
                needs_commit = True
                if error:
                    self.log("[JobThread] Job error: %s", job)
                    self.retry_channel_fetch(c, job.key, job.retry)
                    continue
                try:
                    self.log("[%s] Store channel data", job.key)
                    ch_id = self.backend.store_channel_data(c, data, job)
                except:
                    self.logerror(job.key)
                    self.retry_channel_fetch(c, job.key, job.retry)
                    continue
                # Got the channel data, now enqueue channel video fetch
                self.add_channel_video_fetch(c, ch_id, job.key, job.retry)

            for data, job, error in self.cvideo_fetch_task.process(sleep):
                needs_commit = True
                if error:
                    self.active_channels.remove(job.key)
                    self.log("[JobThread] Job error: %s", job)
                    self.backend.partial_cv_fail(c, job)
                    self.retry_channel_fetch(c, job.key, job.retry)
                    continue
                if data is None:
                    # First entry always None
                    self.active_channels.remove(job.key)
                    self.log('[%s] Finished channel video fetch', job.key)
                    continue
                video_id, video = data
                logkey = job.key + ':' + video_id
                try:
                    self.log("[%s] Storing channel video", logkey)
                    cv_id = self.backend.store_channel_video(c, data, job)
                except:
                    self.logerror(logkey)
                    self.backend.partial_cv_fail(c, job)
                    self.retry_channel_fetch(c, job.key, job.retry)
                    continue
                self.active_videos.add(cv_id)
                self.video_fetch_task.add(VideoDataFetchJob(video_id, cv_id, job.ch_id, 0))

            for data, job, error in self.video_fetch_task.process(sleep):
                needs_commit = True
                self.active_videos.remove(job.cv_id)
                if error:
                    self.log("[JobThread] Job error: %s", job)
                    self.retry_video_fetch(c, job.cv_id, job.retry)
                    continue
                try:
                    self.log("[%s] Storing additional video data", job.key)
                    self.backend.store_additional_video_data(c, data, job)
                except:
                    self.logerror(job.key)
                    self.retry_video_fetch(c, job.cv_id, job.retry)

            if needs_commit:
                try:
                    self.db.commit()
                    retry_commit = False
                except:
                    retry_commit = True
                    self.logerror('JobThread')
            time.sleep(sleep)
    
    def add_channel_video_fetch(self, c, ch_id, channel_id, retry):
        aux = self.backend.get_aux_channel_data(c, ch_id)
        self.cvideo_fetch_task.add(ChannelVideosFetchJob(channel_id, retry, ch_id, aux))

    def retry_channel_fetch(self, c, channel_id, retry_count):
        c.execute('INSERT OR IGNORE INTO channel_fetch_jobs (channel_id, retry) VALUES (?, ?)',
                  (channel_id, retry_count + 1))

    def retry_video_fetch(self, c, cv_id, retry_count):
        c.execute('INSERT OR IGNORE INTO video_meta_fetch_jobs (cv_id, retry) VALUES (?, ?)',
                  (cv_id, retry_count + 1))

    def channel_data_fetch(self, channel_jobs):
        self.log("Fetching %d channels", len(channel_jobs))
        jobs = {j.key: j for j in channel_jobs}
        for id, data in self.backend.get_channel_data(jobs.keys()):
            yield data, jobs[id]

    def channel_video_fetch(self, fetch_jobs):
        assert len(fetch_jobs) == 1
        job = fetch_jobs[0]
        self.log("[%s] Fetching videos for this channel", job.key)
        # Always yield at least once
        yield None, job
        for data in self.backend.get_channel_videos(job.key, job.aux_data):
            yield data, job

    def video_data_fetch(self, video_jobs):
        self.log("Fetching additional video data for %d videos", len(video_jobs))
        jobs = {j.key: j for j in video_jobs}
        for id, data in self.backend.get_additional_video_data(jobs.keys()):
            yield data, jobs[id]

class YoutubeAPIBackend:

    def __init__(self, api_client):
        self.yt = api_client

    def get_channel_data(self, channel_ids):
        for data in self.yt.compose_data(list(channel_ids),
                     self.yt.get_channel_datas, 'snippet', 'contentDetails'):
            yield data['id'], data

    def store_channel_data(self, c, data, job):
        c_id = data['id']
        sn = data['snippet']
        cust_url = sn['customUrl'] if 'customUrl' in sn else None
        pub_at = sn['publishedAt'] if 'publishedAt' in sn else None
        c.execute('INSERT INTO channels (channel_id, title, cust_url, \
                                         description, published_at) \
                                         VALUES (?, ?, ?, ?, ?)',
                  (c_id, sn['title'], cust_url, sn['description'], pub_at))
        ch_id = c.lastrowid
        uploads = data['contentDetails']['relatedPlaylists']['uploads']
        c.execute('INSERT INTO api_channel_data (ch_id, upload_plist) \
                   VALUES (?, ?)', (ch_id, uploads))
        return ch_id

    def get_aux_channel_data(self, c, ch_id):
        uploads, recent, seq = c.execute('SELECT upload_plist, most_recent, seq_num \
                                          FROM api_channel_data WHERE ch_id = ?', (
                                              ch_id,)).fetchone()
        # A job could have partially failed where some videos were added
        # but most_recent was reset. Want to ignore those more recent than most_recent
        ignore = set()
        if recent is not None:
            recent_id, = c.execute('SELECT id FROM channel_video WHERE video_id = ?',
                                  (recent,)).fetchone()
            since_recent = c.execute('SELECT video_id FROM channel_video \
                                      WHERE id > ? AND ch_id = ?',
                                     (recent_id, ch_id)).fetchall()
            for id, in since_recent:
                ignore.add(id)
        return {'playlist': uploads, 'latest_id': recent, 'seq': seq, 'ignore': ignore}

    def get_channel_videos(self, channel_id, aux_data):
        # From get_aux_channel_data
        uploads = aux_data['playlist']
        latest_id = aux_data['latest_id']
        ignore = aux_data['ignore']
        for video in self.yt.get_playlist_items(uploads):
            video_id = video['snippet']['resourceId']['videoId']
            if latest_id == video_id:
                return
            if video_id in ignore:
                continue
            yield video_id, video['snippet']

    def partial_cv_fail(self, c, job):
        # A job partially failed. Therefore have to revert most_recent to before
        # the job started
        aux = job.aux_data
        self.update_most_recent(aux['seq'] + 2, aux['latest_id'], job.ch_id)

    def store_channel_video(self, c, data, job):
        video_id, video = data
        c.execute('INSERT INTO channel_video (ch_id, video_id, title, \
                                              description, published_at) \
                                              VALUES (?, ?, ?, ?, ?)',
                  (job.ch_id, video_id, video['title'],
                   video['description'], video['publishedAt']))
        cv_id = c.lastrowid
        self.update_most_recent(job.aux_data['seq'] + 1, video_id, ch_id)
        return cv_id

    def update_most_recent(self, seq, video_id, ch_id):
        # Sequence number needed because most recent video comes first
        # so we must track this run where seq_num is +1 from previous run
        c.execute('UPDATE api_channel_data SET most_recent = ?, seq_num = ? \
                   WHERE ch_id = ? AND seq_num < ?',
                  (video_id, seq, job.ch_id, seq))

    def get_additional_video_data(self, video_ids):
        for video in self.yt.get_videos(video_ids, 'contentDetails'):
            yield video['id'], video['contentDetails']

    def store_additional_video_data(self, c, data, job):
        duration = iso8601_duration_as_seconds(data['duration'])
        c.execute('UPDATE channel_video SET duration = ? WHERE id = ?', (
            duration, job.cv_id))
        # TODO should upsert instead of ignore
        c.execute('INSERT OR IGNORE INTO api_video_details (cv_id, compressed_json) \
                   VALUES (?, ?)',
                  (job.cv_id, compress_json(data)))

if __name__ == '__main__':
    with open('config.json', 'r') as f:
        config = json.load(f)
    db = sqlite3.connect(config['database']['file'], check_same_thread=False)
    db.execute('PRAGMA foreign_keys = ON')
    f_config = config['metadata_fetcher']
    f = MetadataFetcher(db, f_config['workers'], f_config['max_retry'],
                        APIKeyAuthMode(f_config['youtube_api_key']))
    def interrupt(sig, stack):
        f.stop()
        db.close()
    signal.signal(signal.SIGINT, interrupt)
    f.run()
