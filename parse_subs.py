from xml.dom.minidom import parse as parse_xml

from concurrent.futures import ThreadPoolExecutor
import subprocess
import json

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


def get_ids(channel_id):
    print(channel_id)
    out = subprocess.check_output(['youtube-dl', '-J', '--flat-playlist',
                             'https://www.youtube.com/channel/' + channel_id])
    with open('channels/channel-%s.json' % channel_id, 'wb') as f:
        f.write(out)

def try_get_ids(cid):
    try:
        get_ids(cid)
    except:
        import traceback
        traceback.print_exc()

def get_all_ids(entries):
    with ThreadPoolExecutor() as pool:
        for title, cid, url in entries:
            pool.submit(try_get_ids, cid)
    


if __name__ == '__main__':
    subs = parse_subscriptions('subscription_manager.opml')
    get_all_ids(subs)
