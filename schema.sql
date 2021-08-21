PRAGMA foreign_keys = ON;

CREATE TABLE channels (
    id integer PRIMARY KEY AUTOINCREMENT,
    channel_id text NOT NULL,
    title text NOT NULL,
    cust_url text,
    description text,
    published_at text,
    username text,
    unique(channel_id)
);

CREATE TABLE channel_video (
    id integer PRIMARY KEY AUTOINCREMENT,
    ch_id integer NOT NULL REFERENCES channels(id) ON DELETE CASCADE,
    video_id text NOT NULL,
    title text NOT NULL,
    description text,
    published_at text,
    duration integer,
    unique(video_id)
);

CREATE TABLE api_channel_data (
    ch_id integer NOT NULL PRIMARY KEY REFERENCES channels(id) ON DELETE CASCADE,
    upload_plist text NOT NULL,
    most_recent text REFERENCES channel_video(video_id) ON DELETE SET NULL,
    seq_num integer NOT NULL DEFAULT 0
);

CREATE TABLE api_video_details (
    cv_id integer NOT NULL PRIMARY KEY REFERENCES channel_video(id) ON DELETE CASCADE,
    compressed_json blob NOT NULL
);

CREATE TABLE channel_fetch_jobs (
    channel_id text NOT NULL PRIMARY KEY,
    retry integer NOT NULL DEFAULT 0
);

CREATE TABLE video_meta_fetch_jobs (
    cv_id integer NOT NULL PRIMARY KEY REFERENCES channel_video(id) ON DELETE CASCADE,
    retry integer NOT NULL DEFAULT 0
);

CREATE TABLE video_fetch_jobs (
    video_id text NOT NULL PRIMARY KEY,
    retry integer NOT NULL DEFAULT 0
);

CREATE TABLE fetch_jobs (
    cv_id integer NOT NULL PRIMARY KEY REFERENCES channel_video(id) ON DELETE CASCADE,
    active integer NOT NULL DEFAULT 0,
    priority integer NOT NULL DEFAULT 1000,
    retry integer NOT NULL DEFAULT 0
);

CREATE TABLE stored_video (
    id integer PRIMARY KEY AUTOINCREMENT,
    cv_id integer NOT NULL REFERENCES channel_video(id) ON DELETE RESTRICT,
    video_filename text NOT NULL,
    unique(cv_id)
);

CREATE TABLE video_raw_meta (
    store_id integer NOT NULL PRIMARY KEY REFERENCES stored_video(id) ON DELETE CASCADE,
    compressed_json blob NOT NULL
);

CREATE TABLE thumbnail_file (
    store_id integer NOT NULL PRIMARY KEY REFERENCES stored_video(id) ON DELETE RESTRICT,
    filename text NOT NULL
);

CREATE TABLE subtitle_files (
    store_id integer NOT NULL REFERENCES stored_video(id) ON DELETE RESTRICT,
    language text NOT NULL,
    filename text NOT NULL,
    PRIMARY KEY (store_id, language)
);

CREATE TABLE ia_video (
    video_id text PRIMARY KEY,
    ia_id text NOT NULL
);

CREATE TABLE yt_archive_channels (
    id integer NOT NULL PRIMARY KEY,
    channel_id text,
    username text,
    unique(channel_id),
    unique(username)
);

CREATE TABLE ignored_channels (
    ch_id integer NOT NULL PRIMARY KEY REFERENCES channels(id) ON DELETE CASCADE
);
