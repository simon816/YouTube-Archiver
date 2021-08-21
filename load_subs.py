import json
import sqlite3

from youtube_api import YoutubeAPI, APIKeyAuthMode

script = """
copy($$("#main-link.channel-link").map(e => (sp = e.href.split('/'), sp[sp.length-2] + '/' + sp[sp.length-1])).join('\\n'))
""".strip()

def queue_all_ids(db, channel_ids):
    c = db.cursor()
    for channel_id in channel_ids:
        c.execute('INSERT OR IGNORE INTO channel_fetch_jobs (channel_id) VALUES (?)', (
            channel_id,))
    db.commit()

if __name__ == '__main__':

    with open('config.json', 'r') as f:
        config = json.load(f)

    f_config = config['metadata_fetcher']
    yt = YoutubeAPI(APIKeyAuthMode(f_config['youtube_api_key']))

    print('Paste list ' + script + '\n')
    subs = []
    while True:
        s = input('> ')
        if not s:
            break
        subs.append(s)
    ids = []
    for s in subs:
        ntype, name = s.strip().split('/')
        if ntype == 'user':
            channel = next(yt.get_channel_for_username(name))
            channel_id = channel['id']
        else:
            channel_id = name
        print(channel_id)
        ids.append(channel_id)

    db = sqlite3.connect('youtube.db')
    queue_all_ids(db, ids)
    db.close()
