import os
import json
import html
import subprocess
import signal
import re
import time

from concurrent.futures import ThreadPoolExecutor
from queue import Queue, Empty
from threading import Thread, Semaphore

class RuleLogic:

    types = {}

    def __init__(self, params):
        self.params = params
        self.load(**params)

    def json(self):
        return self.params

    def reject_profile(self, profile):
        return False

    def reject_video(self, video):
        return False

    @classmethod
    def parse(cls, args):
        params = {}
        for a in args:
            k, v = a.split('=', 2)
            k, v = cls.parse_arg(k, v)
            params[k] = v
        return cls(params)

    def parse_arg(self, key, val):
        assert False

class VidCountRule(RuleLogic):

    id = 'vid_count'

    def load(self, mincount=0, maxcount=None):
        self.mincount = mincount
        self.maxcount = maxcount

    def reject_profile(self, profile):
        count = len(profile.get_videos())
        return count < self.mincount or \
            (self.maxcount is not None and count > self.maxcount)

    @staticmethod
    def parse_arg(key, value):
        return key, int(value)

class VidTitleRegex(RuleLogic):

    id = 't_regex'

    def load(self, pattern):
        self.regex = re.compile(pattern)

    def reject_video(self, video):
        return self.regex.search(video.title) is not None

    @staticmethod
    def parse_arg(key, value):
        return key, value

class IAExistRule(RuleLogic):

    id = 'in_ia'

    def load(self, in_ia=True):
        self.in_ia = in_ia

    def reject_video(self, video):
        return video.in_ia == self.in_ia

    @staticmethod
    def parse_arg(key, value):
        return key, {
            'true': True,
            'false': False
        }[value]

RuleLogic.types[VidCountRule.id] = VidCountRule
RuleLogic.types[VidTitleRegex.id] = VidTitleRegex
RuleLogic.types[IAExistRule.id] = IAExistRule

class Rule:

    def __init__(self, logic=None):
        self.logic = logic

    def load(self, data):
        self.logic = RuleLogic.types[data['type']](data['params'])

    def json(self):
        return {
            'type': self.logic.id,
            'params': self.logic.json()
        }

    def reject_profile(self, profile):
        return self.logic.reject_profile(profile)

    def reject_video(self, video):
        return self.logic.reject_video(video)

    @staticmethod
    def parse(args):
        logic = RuleLogic.types[args[0]].parse(args[1:])
        return Rule(logic)

class Profile:

    def __init__(self, coordinator, id):
        self.coordinator = coordinator
        self.ch_id = id
        self.channel_name = None
        self.videos = {}
        self.rules = []

    @staticmethod
    def from_channel_meta(coordinator, channel_meta):
        channel_ref = channel_meta['uploader_id']
        if channel_ref.startswith('UC'):
            ch_id = channel_ref
        else:
            ch_id = 'UC' + channel_meta['id'][2:]
        channel_name = html.unescape(channel_meta['uploader'])
        profile = Profile(coordinator, ch_id)
        profile.channel_name = channel_name
        for e in channel_meta['entries']:
            vid = Video.from_channel_meta(profile, e)
            if vid.id not in profile.videos:
                profile.add_video(vid)
        return profile

    def add_video(self, vid):
        assert vid.id not in self.videos, "Channel: %s, Video: %s" % (self.ch_id, vid.id)
        assert vid.profile == self
        self.videos[vid.id] = vid

    def get_videos(self):
        return self.videos.values()

    def add_rule(self, rule):
        self.rules.append(rule)

    def remove_rule(self, index):
        del self.rules[index]

    def get_rules(self):
        return self.rules
        
    def load(self, entry):
        self.channel_name = entry['name']
        
        for rule_data in entry['rules']:
            rule = Rule()
            rule.load(rule_data)
            self.add_rule(rule)

        for id, vid_data in entry['videos'].items():
            vid = Video(self, id)
            vid.load(vid_data)
            self.add_video(vid)

    def json(self):
        videos = {}
        for video in self.videos.values():
            videos[video.id] = video.json()
        return {
            'name': self.channel_name,
            'videos': videos,
            'rules': [rule.json() for rule in self.rules]
        }

    def get_filtered_videos(self, global_rules):
        all_rules = global_rules + self.rules
        for rule in all_rules:
            if rule.reject_profile(self):
                return []
        for video in self.videos.values():
            allow = True
            for rule in all_rules:
                if rule.reject_video(video):
                    allow = False
                    break
            if allow:
                yield video

    def resume(self, global_rules):
        for video in self.get_filtered_videos(global_rules):
            video.fetch()

    def add_job(self, job):
        self.coordinator.add_job(job)

class Video:

    def __init__(self, profile, id):
        self.profile = profile
        self.id = id
        self.title = None
        self.in_ia = None
        self.video_data = None
        self.loaded = False

    @staticmethod
    def from_channel_meta(profile, ref):
        vid = Video(profile, ref['id'])
        vid.title = ref['title']
        vid.loaded = True
        return vid

    def set_in_ia(self, in_ia):
        self.in_ia = in_ia

    def load(self, entry):
        self.title = entry['title']
        self.video_data = entry['data']
        self.loaded = True

    def json(self):
        assert self.loaded
        return {
            'id': self.id,
            'title': self.title,
            'in_ia': self.in_ia,
            'data': self.video_data
        }

    def fetch(self):
        assert self.loaded
        if self.video_data:
            return
        self.profile.add_job(self)

    def _download(self, logqueue, *args):
        try:
            self._download0(logqueue, *args)
        except:
            import traceback
            logqueue.put(traceback.format_exc())

    def _download0(self, logqueue, processlist, callback):
        logqueue.put("Begin " + self.id)
        outfmt = r'Videos/%(uploader)s/%(upload_date)s - %(title)s - %(id)s.%(ext)s'
        fmt = 'bestvideo[height <=? 1080]+bestaudio/best[height <=? 1080]/best'
        url = 'https://www.youtube.com/watch?v=%s' % self.id
        p = subprocess.Popen(['youtube-dl',
                '--print-json',
                '--cache-dir', './yt-dl-cache/',
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
            ], stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                universal_newlines=True)
        processlist.append(p)
        outs, errs = p.communicate()
        logqueue.put("End " + self.id)
        if p.returncode == 0:
            logqueue.put("Errors: " + errs)
            ret = json.loads(outs)
            fn = ret['_filename']
            logqueue.put("File: " + fn)
            self.video_data = {
                'filename': fn,
                'metadata': ret
            }
            callback(self)
        else:
            logqueue.put("Process error: " + errs)

class Coordinator:

    def __init__(self, state_db_file):
        self.state_file = state_db_file
        self.rules = []
        self.profiles = {}

    def clear(self):
        self.profiles = {}

    def add_profile(self, profile):
        assert profile.ch_id not in self.profiles
        self.profiles[profile.ch_id] = profile

    def get_profiles(self):
        return self.profiles.values()

    def add_rule(self, rule):
        self.rules.append(rule)

    def remove_rule(self, index):
        del self.rules[index]

    def get_rules(self):
        return self.rules

    def load_state(self):
        self.clear()
        with open(self.state_file, 'r') as f:
            state = json.load(f)

        if 'rules' in state:
            for rule_data in state['rules']:
                rule = Rule()
                rule.load(rule_data)
                self.add_rule(rule)
            
        if 'profiles' in state:
            for id, dat in state['profiles'].items():
                profile = Profile(self, id)
                profile.load(dat)
                self.add_profile(profile)

    def save_state(self):
        os.rename(self.state_file, self.state_file + '_old')
        with open(self.state_file, 'w') as f:
            profiles = {}
            for profile in self.profiles.values():
                profiles[profile.ch_id] = profile.json()
            json.dump({
                'profiles': profiles,
                'rules': [rule.json() for rule in self.rules]
            }, f)

    def run(self):
        self.logqueue = Queue()
        self.running = True
        logthread = Thread(target=self.logprint)
        savethread = Thread(target=self.state_saver_thread)
        self.pool = ThreadPoolExecutor(max_workers=3)
        self.futures = []
        self.processlist = []
        self.state_dirty = False
        self.dirty_lock = Semaphore()
        logthread.start()
        savethread.start()
        for profile in self.profiles.values():
            profile.resume(self.rules)
        self.pool.shutdown()
        self.logqueue.join()
        self.running = False
        logthread.join()
        savethread.join()
        self.save_state()

    def interrupt(self, sig, stack):
        self.logqueue.put("Quitting")
        for f in self.futures:
            f.cancel()
        self.logqueue.put("Closing youtube-dl")
        for p in self.processlist:
            p.send_signal(signal.SIGINT)
        self.logqueue.put("Wait for terminate")
        for p in self.processlist:
            p.wait()

    def logprint(self):
        while self.running:
            try:
                item = self.logqueue.get(True, 1)
                print(item)
                self.logqueue.task_done()
            except Empty:
                pass

    def state_saver_thread(self):
        while self.running:
            try:
                self.dirty_lock.acquire()
                if self.state_dirty:
                    self.state_dirty = False
                    self.save_state()
            finally:
                self.dirty_lock.release()
            time.sleep(5)

    def success_cb(self, video):
        try:
            self.dirty_lock.acquire()
            self.state_dirty = True
        finally:
            self.dirty_lock.release()

    def add_job(self, job):
        self.futures.append(self.pool.submit(job._download, self.logqueue,
                                             self.processlist, self.success_cb))

if __name__ == '__main__':
    app = Coordinator('state.json')
    signal.signal(signal.SIGINT, app.interrupt)
    app.load_state()
    app.run()


