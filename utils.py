
def yt_dl(args):
    return ['youtube-dl', '--cache-dir', './yt-dl-cache/'] + list(args)
