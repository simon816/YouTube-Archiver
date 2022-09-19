import zlib
import json
import re

def yt_dl(args):
    if True: #  use yt-dlp
        return ['yt-dlp',
                '--cache-dir', './yt-dl-cache/',
                '--compat-options', 'all',
                '--extractor-args', 'youtube:player_client=android',
                ] + list(args)
    return ['youtube-dl',
            '--cache-dir', './yt-dl-cache/',
            ] + list(args)

def compress_json(data):
    return zlib.compress(json.dumps(data).encode('utf8'))

def uncompress_json(data):
    return json.loads(zlib.decompress(data).decode('utf8'))

# from https://stackoverflow.com/a/35936407
def iso8601_duration_as_seconds(d):
    if d[0] != 'P':
        raise ValueError('Not an ISO 8601 Duration string')
    seconds = 0
    # split by the 'T'
    for i, item in enumerate(d.split('T')):
        for number, unit in re.findall( '(?P<number>\d+)(?P<period>S|M|H|D|W|Y)', item ):
            # print '%s -> %s %s' % (d, number, unit )
            number = int(number)
            this = 0
            if unit == 'Y':
                this = number * 31557600 # 365.25
            elif unit == 'W':
                this = number * 604800
            elif unit == 'D':
                this = number * 86400
            elif unit == 'H':
                this = number * 3600
            elif unit == 'M':
                # ambiguity ellivated with index i
                if i == 0:
                    this = number * 2678400 # assume 30 days
                    # print "MONTH!"
                else:
                    this = number * 60
            elif unit == 'S':
                this = number
            seconds = seconds + this
    return seconds
