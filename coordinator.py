import json
import sqlite3
import os

video_query = r"""
SELECT cv1.id
FROM channel_video cv1
JOIN (
    SELECT cv2.ch_id
    FROM channel_video cv2
    JOIN channels c ON c.id = cv2.ch_id
    WHERE c.channel_id NOT IN (
        SELECT channel_id
        FROM yt_archive_channels
        WHERE channel_id is NOT NULL
    ) 
    AND c.username NOT IN (
        SELECT username
        FROM yt_archive_channels
        WHERE username is NOT NULL
    )
    AND c.id NOT IN (SELECT ch_id FROM ignored_channels)
    AND (? OR cv2.duration < ?)
    GROUP BY cv2.ch_id 
    HAVING count(cv2.id) <= ? AND count(cv2.id) >= ?
) t ON cv1.ch_id = t.ch_id 
WHERE 
    cv1.video_id NOT IN (SELECT video_id FROM ia_video)
    AND cv1.id NOT IN (SELECT cv_id FROM stored_video) 
    AND cv1.duration < ?
    AND (? OR datetime(cv1.published_at) >= date('now', ?))
    ORDER BY datetime(cv1.published_at) DESC
"""

insert_jobs = 'INSERT OR IGNORE INTO fetch_jobs (cv_id) '

def video_selection_args(max_cv_count, min_cv_count, max_duration,
                         years_back=None, cv_candidate_duration=None):
    date_mod = ('-%d year' % years_back) if years_back is not None else None
    return (1 if cv_candidate_duration is None else 0, cv_candidate_duration,
            max_cv_count, min_cv_count, max_duration,
            1 if years_back is None else 0, date_mod)

def queue_candidates(db):
    c = db.cursor()
    # All queries check whether in IA or YA
    # Channels with less than 100 videos after subtracting videos of length > 2hr
    c.execute(insert_jobs + video_query, video_selection_args(100, 1, 60*60*2, None, 60*60*2))
    # Channels totalling less than 200 videos where length < 30min and published not before 4 years ago
    c.execute(insert_jobs + video_query, video_selection_args(200, 101, 60*30, 4))
    # Channels totalling less than 300 videos where length < 20min and published not before 3 years ago
    c.execute(insert_jobs + video_query, video_selection_args(300, 201, 60*20, 3))
    # Channels totalling less than 3000 videos where length < 15min and published not before 2 years ago
    c.execute(insert_jobs + video_query, video_selection_args(3000, 301, 60*15, 2))

    db.commit()

def queue_history(db):
    c = db.cursor()
    ids = set()
    for line in open('history.txt', 'r'):
        v_id = line.strip()
        ids.add(v_id)

    c.executemany("""
        INSERT OR IGNORE INTO fetch_jobs (cv_id, priority)
            SELECT cv.id, cv.duration / 60
            FROM channel_video cv
            JOIN channels c ON c.id = cv.ch_id
            WHERE video_id = ?
            -- AND c.id NOT IN (SELECT ch_id FROM ignored_channels)
            AND c.channel_id NOT IN (
                SELECT channel_id
                FROM yt_archive_channels
                WHERE channel_id is NOT NULL
            )
            AND c.username NOT IN (
                SELECT username
                FROM yt_archive_channels
                WHERE username is NOT NULL
            )
            AND cv.video_id NOT IN (SELECT video_id FROM ia_video)
            AND cv.id NOT IN (SELECT cv_id FROM stored_video)
    """, [(i,) for i in ids])
    db.commit()

def dur_str(seconds):
    mins, secs = divmod(seconds, 60)
    hours, mins = divmod(mins, 60)
    return '%d:%d:%d' % (hours, mins, secs)

def stats(db):
    c = db.cursor()
    allfetched = c.execute('SELECT video_filename, duration FROM stored_video sv JOIN channel_video cv ON cv.id = cv_id').fetchall()
    total_size = 0
    total_dur = 0
    max_dur = 0
    for filename, duration in allfetched:
        path = os.path.join('/media/bd/sinkhole/YouTube/', filename)
        size = os.path.getsize(path)
        total_size += size
        total_dur += duration
        max_dur = max(duration, max_dur)
    count = len(allfetched)

    print('Total size: %.1fGB' % (total_size / 1024 / 1024 / 1024))
    print('Count: %d' % count)
    print('Average file size: %.1fMB' % ((total_size / count) / 1024 / 1024))
    print('Total duration: %s' % dur_str(total_dur))
    print('Average duration per video: %s' % dur_str(total_dur // count))
    print('Average data per second: %.2fKB/s' % (total_size / total_dur / 1024))
    print('Average seconds per megabyte: %.2fs/MB' % (total_dur / (total_size / 1024 / 1024)))
    print('Longest video: %s' % dur_str(max_dur))

def check_files(db):
    c = db.cursor()
    root = '/media/bd/sinkhole/YouTube/'
    files = c.execute('select video_filename from stored_video union select filename from thumbnail_file union select filename from subtitle_files').fetchall()
    fileset = set()
    for (filename,) in files:
        path = os.path.join(root, filename)
        if not os.path.isfile(path):
            print("Missing", filename)
        fileset.add(path)
    for dir, dirs, files in os.walk(os.path.join(root, 'Videos')):
        for file in files:
            path = os.path.join(dir, file)
            if path not in fileset:
                mtime = os.path.getmtime(path)
                import datetime
                print("Unattributed", datetime.datetime.fromtimestamp(mtime), path)

def format_info(db):
    c = db.cursor()
    from utils import uncompress_json
    videos = c.execute('select compressed_json from video_raw_meta').fetchall()
    from collections import defaultdict
    formats = defaultdict(lambda: 0)
    for (data,) in videos:
        vfmt = []
        meta = uncompress_json(data)
        for fmt in meta['format'].split('+'):
            fm = meta['requested_formats'] if 'requested_formats' in meta else meta['formats']
            for f in fm:
                if f['format'] == fmt:
                    vfmt.append(f)
                if f['vcodec'] == 'none':
                    f['vcodec'] = None
                if f['acodec'] == 'none':
                    f['acodec'] = None
        fmt = tuple([f['vcodec'] or f['acodec'] for f in vfmt])
        formats[fmt] += 1
    for fmt, count in sorted(formats.items(), key=lambda i: i[1]):
        print(count, fmt)

if __name__ == '__main__':
    with open('config.json', 'r') as f:
        config = json.load(f)
    db = sqlite3.connect(config['database']['file'], check_same_thread=False)
    db.execute('PRAGMA foreign_keys = ON')
    import sys
    action = sys.argv[1]
    if action == 'stats':
        stats(db)
    elif action == 'queue':
        queue_candidates(db)
    elif action == 'check':
        check_files(db)
    elif action == 'fmtinfo':
        format_info(db)
    elif action == 'queue-from-history':
        queue_history(db)
    else:
        print("Unknown action", action)
    db.close()
