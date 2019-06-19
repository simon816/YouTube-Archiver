CREATE TABLE fetch_settings (
    workers integer NOT NULL,
    bandwidth integer NOT NULL,
    max_retry integer NOT NULL
);

CREATE TABLE channel (
    id integer PRIMARY KEY AUTOINCREMENT,
    channel_id text NOT NULL,
    name text NOT NULL,
    username text,
    unique(channel_id)
);

CREATE TABLE channel_video (
    id integer PRIMARY KEY AUTOINCREMENT,
    channel_id integer NOT NULL REFERENCES channel(id),
    video_id text NOT NULL,
    upload_date text NOT NULL,
    title text,
    description text,
    duration integer NOT NULL,
    unique(video_id)
);

CREATE TABLE fetch_jobs (
    cvideo_id integer NOT NULL REFERENCES channel_video(id),
    retry integer NOT NULL DEFAULT 0
);

CREATE TABLE channel_fetch_jobs (
    channel_id text NOT NULL PRIMARY KEY
);

CREATE TABLE stored_video (
    id integer PRIMARY KEY AUTOINCREMENT,
    cvideo_id integer NOT NULL REFERENCES channel_video(id),
    video_filename text NOT NULL,
    unique(cvideo_id)
);

CREATE TABLE video_raw_meta (
    store_id integer NOT NULL PRIMARY KEY REFERENCES stored_video(id),
    compressed_json blob NOT NULL
);

CREATE TABLE thumbnail_file (
    store_id integer NOT NULL PRIMARY KEY REFERENCES stored_video(id),
    filename text NOT NULL
);

CREATE TABLE subtitle_files (
    store_id integer NOT NULL REFERENCES stored_video(id),
    language text NOT NULL,
    filename text NOT NULL,
    PRIMARY KEY (store_id, language)
);

CREATE TABLE ia_video (
    video_id text PRIMARY KEY
);
