from xml.dom.minidom import parse as parse_xml
import sqlite3

def parse_subscriptions(file):
    doc = parse_xml(file)
    outlines = doc.getElementsByTagName('outline')
    entries = []
    for outline in outlines:
        title = outline.getAttribute('title')
        if title == 'YouTube Subscriptions':
            continue
        url = outline.getAttribute('xmlUrl')
        channel_id = url[url.index('channel_id=') + 11:]
        entries.append((title, channel_id, url))
    return entries

def queue_all_ids(db, subs):
    c = db.cursor()
    for title, channel_id, url in subs:
        c.execute('INSERT OR IGNORE INTO channel_fetch_jobs (channel_id) VALUES (?)', (
            channel_id,))
    db.commit()

if __name__ == '__main__':
    subs = parse_subscriptions('subscription_manager.opml')
    db = sqlite3.connect('youtube.db')
    queue_all_ids(db, subs)
    db.close()
