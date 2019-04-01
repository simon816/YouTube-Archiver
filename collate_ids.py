import os
import json
import html

import get_ia

files = os.listdir('channels')

ia_ids = get_ia.get_ia_ids()

out = []

grand_total = 0

for file in files:
    with open('channels/' + file, 'r') as f:
        data = json.load(f)
    channel = data['uploader_id']
    plist_id = data['id']
    playlist = data['title']
    channel_name = html.unescape(data['uploader'])
    total_vids = len(data['entries'])
    grand_total += total_vids
    found = sum(1 for e in data['entries'] if e['id'] in ia_ids)
    percent = (found / total_vids) * 100 if total_vids else -1
    out.append((total_vids, percent, channel_name, channel))

for total, percent, name, channel in sorted(out, reverse=True):
    print(total, '%.3f%%' % percent, channel, name)

print(grand_total)
