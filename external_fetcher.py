from urllib.request import urlopen, Request
from urllib.parse import urlencode, urlparse, parse_qs
from urllib.error import HTTPError
from html.parser import HTMLParser
import json
import sqlite3

class LinkFetcher(HTMLParser):

    def __init__(self):
        super().__init__()
        self.links = []

    def handle_starttag(self, tag, attrs):
        if tag == 'a':
            adict = dict(attrs)
            self.add_link(adict['href'])

    def add_link(self, link):
        # ignore these
        if link.startswith('?') or link.startswith('/'):
            return
        self.links.append(link)

def fetch_ya_monitored(password):
    with urlopen(Request('https://ya.borg.xyz/logs/dl/', headers={
            'Authorization': 'Basic ' + password,
        })) as f:
        lf = LinkFetcher()
        lf.feed(f.read().decode('utf8'))
    usernames, channel_ids = [], []
    for link in lf.links:
        # trim "/"
        link = link[:-1]
        if len(link) == 24 and link.startswith('UC'):
            channel_ids.append(link)
        else:
            usernames.append(link)
    return usernames, channel_ids

def store_ya_monitored(usernames, channel_ids, db):
    c = db.cursor()
    for channel_id in channel_ids:
        c.execute('INSERT OR IGNORE INTO yt_archive_channels (channel_id) VALUES (?)',
                  (channel_id,))
    for username in usernames:
        c.execute('INSERT OR IGNORE INTO yt_archive_channels (username) VALUES (?)',
                  (username,))
    db.commit()

def ia_iterator(ia_query, fields, count=10_000):
    # See https://archive.org/help/aboutsearch.htm
    query = {
        'q': ia_query,
        'fields': ','.join(fields),
        'count': count
    }
    while True:
        url = 'https://archive.org/services/search/v1/scrape?' + urlencode(query)
        try:
            with urlopen(url) as f:
                data = json.load(f)
        except HTTPError as e:
            print(e.read())
            raise e
        print("Got batch of", len(data['items']))
        yield from data['items']
        cursor = query['cursor'] = data.get('cursor', None)
        if cursor is None:
            break

def filter_youtube(results):
    for result in results:
        id = result['identifier']
        if 'youtube-id' in result:
            video_id = result['youtube-id']
            if type(video_id) == list:
                video_id = video_id[0]
            if len(video_id) <= 11:
                yield video_id, id 
        elif id.startswith('youtube-'):
            yield id[8:], id
        elif 'url' in result or 'originalurl' in result:
            try:
                url = urlparse(result.get('url') or result['originalurl'])
            except ValueError:
                continue
            if url.netloc == 'www.youtube.com' and url.path == '/watch':
                q = parse_qs(url.query)
                if 'v' in q:
                    yield q['v'][0], id

def do_ia_fetch(db):
    ia_query = 'collection:(archiveteam_youtube OR community_media OR youtubearchive) AND mediatype:(movies)'
    fields = ['identifier', 'url', 'youtube-id', 'originalurl']
    c = db.cursor()
    for video_id, ia_id in filter_youtube(ia_iterator(ia_query, fields)):
        retry = 3
        while retry > 0:
            try:
                c.execute('INSERT OR IGNORE INTO ia_video (video_id, ia_id) VALUES (?, ?)', (
                    video_id, ia_id))
                break
            except:
                retry -= 1
                if retry == 0:
                    raise

    db.commit()

if __name__ == '__main__':
    with open('config.json', 'r') as f:
        config = json.load(f)
    db = sqlite3.connect(config['database']['file'], check_same_thread=False)
    db.execute('PRAGMA foreign_keys = ON')
    import sys
    action = sys.argv[1]
    if action == 'ya':
        usernames, channel_ids = fetch_ya_monitored(config['ya_password'])
        store_ya_monitored(usernames, channel_ids, db)
    elif action == 'ia':
        do_ia_fetch(db)
    else:
        print("Unknown action", action)
        exit(1)
