-- Init schema for a normalized Spotify-like dataset (PostgreSQL)
-- Version: without import_runs and without snapshot tables
-- Safe to run multiple times (uses IF NOT EXISTS where sensible).

BEGIN;

-- Optional: keep everything in a dedicated schema
CREATE SCHEMA IF NOT EXISTS spotify;
SET search_path = spotify, public;

-- --------------------
-- DICTIONARIES
-- --------------------

CREATE TABLE IF NOT EXISTS genres (
  genre_id     BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  name         VARCHAR(100) NOT NULL UNIQUE,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS markets (
  market_id     BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  country_code  VARCHAR(2) UNIQUE, -- ISO-3166-1 alpha-2 (nullable for Global)
  name          VARCHAR(80) NOT NULL
);

-- --------------------
-- CORE ENTITIES
-- --------------------

CREATE TABLE IF NOT EXISTS artists (
  artist_id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  name               VARCHAR(255) NOT NULL,
  raw_genres_text    VARCHAR(500),
  created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at         TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_artists_name ON artists (name);

CREATE TABLE IF NOT EXISTS albums (
  album_id               BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  spotify_album_id       VARCHAR(22) UNIQUE,
  name                   VARCHAR(255) NOT NULL,
  album_type             VARCHAR(30),
  release_date           DATE,
  total_tracks           INT,
  created_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at             TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_albums_name ON albums (name);
CREATE INDEX IF NOT EXISTS idx_albums_release_date ON albums (release_date);

CREATE TABLE IF NOT EXISTS tracks (
  track_id         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  spotify_track_id VARCHAR(22) NOT NULL UNIQUE,
  name             VARCHAR(255) NOT NULL,
  explicit         BOOLEAN,
  duration_min     NUMERIC(6,3),
  disc_number      SMALLINT,
  track_number     SMALLINT,
  isrc             VARCHAR(15) UNIQUE,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at       TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_tracks_name ON tracks (name);

-- --------------------
-- RELATION TABLES (M:N)
-- --------------------

CREATE TABLE IF NOT EXISTS artist_genres (
  artist_id BIGINT NOT NULL,
  genre_id  BIGINT NOT NULL,
  PRIMARY KEY (artist_id, genre_id),
  CONSTRAINT fk_artist_genres_artist
    FOREIGN KEY (artist_id) REFERENCES artists (artist_id) ON DELETE CASCADE,
  CONSTRAINT fk_artist_genres_genre
    FOREIGN KEY (genre_id)  REFERENCES genres  (genre_id)  ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS album_artists (
  album_id     BIGINT NOT NULL,
  artist_id    BIGINT NOT NULL,
  artist_order SMALLINT,
  PRIMARY KEY (album_id, artist_id),
  CONSTRAINT fk_album_artists_album
    FOREIGN KEY (album_id)  REFERENCES albums  (album_id)  ON DELETE CASCADE,
  CONSTRAINT fk_album_artists_artist
    FOREIGN KEY (artist_id) REFERENCES artists (artist_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS track_artists (
  track_id     BIGINT NOT NULL,
  artist_id    BIGINT NOT NULL,
  artist_order SMALLINT,
  PRIMARY KEY (track_id, artist_id),
  CONSTRAINT fk_track_artists_track
    FOREIGN KEY (track_id)  REFERENCES tracks  (track_id)  ON DELETE CASCADE,
  CONSTRAINT fk_track_artists_artist
    FOREIGN KEY (artist_id) REFERENCES artists (artist_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS track_albums (
  track_id   BIGINT NOT NULL,
  album_id   BIGINT NOT NULL,
  is_primary BOOLEAN NOT NULL DEFAULT TRUE,
  PRIMARY KEY (track_id, album_id),
  CONSTRAINT fk_track_albums_track
    FOREIGN KEY (track_id) REFERENCES tracks (track_id) ON DELETE CASCADE,
  CONSTRAINT fk_track_albums_album
    FOREIGN KEY (album_id) REFERENCES albums (album_id) ON DELETE CASCADE
);

-- --------------------
-- AUDIO FEATURES (1:1)
-- --------------------

CREATE TABLE IF NOT EXISTS audio_features (
  track_id          BIGINT PRIMARY KEY,
  danceability      NUMERIC(4,3),
  energy            NUMERIC(4,3),
  key               SMALLINT,
  mode              SMALLINT,
  loudness          NUMERIC(6,3),
  speechiness       NUMERIC(4,3),
  acousticness      NUMERIC(4,3),
  instrumentalness  NUMERIC(6,5),
  liveness          NUMERIC(4,3),
  valence           NUMERIC(4,3),
  tempo             NUMERIC(6,2),
  time_signature    SMALLINT,
  CONSTRAINT fk_audio_features_track
    FOREIGN KEY (track_id) REFERENCES tracks (track_id) ON DELETE CASCADE
);

-- --------------------
-- CHARTS
-- --------------------
-- (kept; remove if your dataset doesn't have positions/streams per date/market)

CREATE TABLE IF NOT EXISTS charts (
  chart_id   BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  provider   VARCHAR(40) NOT NULL DEFAULT 'spotify',
  name       VARCHAR(120) NOT NULL,
  chart_type VARCHAR(40),
  market_id  BIGINT,
  CONSTRAINT fk_charts_market
    FOREIGN KEY (market_id) REFERENCES markets (market_id) ON DELETE SET NULL,
  CONSTRAINT uq_charts_unique UNIQUE (provider, name, market_id)
);

CREATE TABLE IF NOT EXISTS chart_entries (
  chart_entry_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  chart_id       BIGINT NOT NULL,
  track_id       BIGINT NOT NULL,
  chart_date     DATE NOT NULL,
  position       SMALLINT NOT NULL,
  streams        BIGINT,
  CONSTRAINT fk_chart_entries_chart
    FOREIGN KEY (chart_id) REFERENCES charts (chart_id) ON DELETE CASCADE,
  CONSTRAINT fk_chart_entries_track
    FOREIGN KEY (track_id) REFERENCES tracks (track_id) ON DELETE CASCADE,
  CONSTRAINT uq_chart_entries_unique UNIQUE (chart_id, track_id, chart_date),
  CONSTRAINT chk_chart_position_positive CHECK (position > 0),
  CONSTRAINT chk_chart_streams_nonneg CHECK (streams IS NULL OR streams >= 0)
);

CREATE INDEX IF NOT EXISTS idx_chart_entries_chart_date
  ON chart_entries (chart_id, chart_date);

CREATE INDEX IF NOT EXISTS idx_chart_entries_track_date
  ON chart_entries (track_id, chart_date);

COMMIT;

-- Notes:
-- - If you don't have chart data in the dataset, you can also remove: markets, charts, chart_entries.
-- - If you guarantee track->album is always 1:1, you can drop track_albums and add tracks.album_id FK.