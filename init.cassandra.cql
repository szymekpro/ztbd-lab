-- Init schema for a normalized Spotify-like dataset (Cassandra)
-- Version: aligned by table/column scope with init.postgres.sql
-- Notes:
-- - Cassandra does not support SQL foreign keys/check constraints.
-- - This script mirrors structure and key access patterns with Cassandra primitives.

CREATE KEYSPACE IF NOT EXISTS spotify
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE spotify;

-- --------------------
-- DICTIONARIES
-- --------------------

CREATE TABLE IF NOT EXISTS genres (
  genre_id bigint,
  name text,
  created_at timestamp,
  PRIMARY KEY (genre_id)
);

CREATE TABLE IF NOT EXISTS markets (
  market_id bigint,
  country_code text,
  name text,
  PRIMARY KEY (market_id)
);

-- --------------------
-- CORE ENTITIES
-- --------------------

CREATE TABLE IF NOT EXISTS artists (
  artist_id bigint,
  name text,
  raw_genres_text text,
  created_at timestamp,
  updated_at timestamp,
  PRIMARY KEY (artist_id)
);

CREATE TABLE IF NOT EXISTS albums (
  album_id bigint,
  spotify_album_id text,
  name text,
  album_type text,
  release_date date,
  total_tracks int,
  created_at timestamp,
  updated_at timestamp,
  PRIMARY KEY (album_id)
);

CREATE TABLE IF NOT EXISTS tracks (
  track_id bigint,
  spotify_track_id text,
  name text,
  explicit boolean,
  duration_min decimal,
  disc_number smallint,
  track_number smallint,
  isrc text,
  created_at timestamp,
  updated_at timestamp,
  PRIMARY KEY (track_id)
);

-- --------------------
-- RELATION TABLES (M:N)
-- --------------------

CREATE TABLE IF NOT EXISTS artist_genres (
  artist_id bigint,
  genre_id bigint,
  PRIMARY KEY (artist_id, genre_id)
);

CREATE TABLE IF NOT EXISTS album_artists (
  album_id bigint,
  artist_id bigint,
  artist_order smallint,
  PRIMARY KEY (album_id, artist_id)
);

CREATE TABLE IF NOT EXISTS track_artists (
  track_id bigint,
  artist_id bigint,
  artist_order smallint,
  PRIMARY KEY (track_id, artist_id)
);

CREATE TABLE IF NOT EXISTS track_albums (
  track_id bigint,
  album_id bigint,
  is_primary boolean,
  PRIMARY KEY (track_id, album_id)
);

-- --------------------
-- AUDIO FEATURES (1:1)
-- --------------------

CREATE TABLE IF NOT EXISTS audio_features (
  track_id bigint,
  danceability decimal,
  energy decimal,
  key smallint,
  mode smallint,
  loudness decimal,
  speechiness decimal,
  acousticness decimal,
  instrumentalness decimal,
  liveness decimal,
  valence decimal,
  tempo decimal,
  time_signature smallint,
  PRIMARY KEY (track_id)
);

-- --------------------
-- CHARTS
-- --------------------

CREATE TABLE IF NOT EXISTS charts (
  chart_id bigint,
  provider text,
  name text,
  chart_type text,
  market_id bigint,
  PRIMARY KEY (chart_id)
);

CREATE TABLE IF NOT EXISTS chart_entries (
  chart_id bigint,
  chart_date date,
  track_id bigint,
  chart_entry_id bigint,
  position smallint,
  streams bigint,
  PRIMARY KEY ((chart_id), chart_date, track_id)
);

-- --------------------
-- SECONDARY INDEXES
-- --------------------

CREATE INDEX IF NOT EXISTS idx_genres_name ON genres (name);
CREATE INDEX IF NOT EXISTS idx_markets_country_code ON markets (country_code);

CREATE INDEX IF NOT EXISTS idx_artists_name ON artists (name);

CREATE INDEX IF NOT EXISTS idx_albums_spotify_album_id ON albums (spotify_album_id);
CREATE INDEX IF NOT EXISTS idx_albums_name ON albums (name);
CREATE INDEX IF NOT EXISTS idx_albums_release_date ON albums (release_date);

CREATE INDEX IF NOT EXISTS idx_tracks_spotify_track_id ON tracks (spotify_track_id);
CREATE INDEX IF NOT EXISTS idx_tracks_name ON tracks (name);
CREATE INDEX IF NOT EXISTS idx_tracks_isrc ON tracks (isrc);

CREATE INDEX IF NOT EXISTS idx_charts_market_id ON charts (market_id);
CREATE INDEX IF NOT EXISTS idx_chart_entries_track_id ON chart_entries (track_id);

