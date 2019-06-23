from urllib.request import urlopen
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

def fetch_ya_monitored():
    with urlopen('https://ya.borg.xyz/logs/dl/') as f:
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

if __name__ == '__main__':
    with open('config.json', 'r') as f:
        config = json.load(f)
    db = sqlite3.connect(config['database']['file'], check_same_thread=False)
    db.execute('PRAGMA foreign_keys = ON')
    usernames, channel_ids = fetch_ya_monitored()
    store_ya_monitored(usernames, channel_ids, db)
