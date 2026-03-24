-- Init schema for a normalized Spotify-like dataset (MariaDB)
-- Version: aligned with init.postgres.sql (PostgreSQL structure)
-- Safe to run multiple times where MariaDB supports IF NOT EXISTS.

START TRANSACTION;

CREATE DATABASE IF NOT EXISTS spotify;
USE spotify;

-- --------------------
-- DICTIONARIES
-- --------------------

CREATE TABLE IF NOT EXISTS genres (
  genre_id     BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  name         VARCHAR(100) NOT NULL UNIQUE,
  created_at   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS markets (
  market_id     BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  country_code  VARCHAR(2) UNIQUE,
  name          VARCHAR(80) NOT NULL
) ENGINE=InnoDB;

-- --------------------
-- CORE ENTITIES
-- --------------------

CREATE TABLE IF NOT EXISTS artists (
  artist_id          BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  name               VARCHAR(255) NOT NULL,
  raw_genres_text    VARCHAR(500),
  created_at         DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at         DATETIME NULL
) ENGINE=InnoDB;

CREATE INDEX IF NOT EXISTS idx_artists_name ON artists (name);

CREATE TABLE IF NOT EXISTS albums (
  album_id               BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  spotify_album_id       VARCHAR(22) UNIQUE,
  name                   VARCHAR(255) NOT NULL,
  album_type             VARCHAR(30),
  release_date           DATE,
  total_tracks           INT,
  created_at             DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at             DATETIME NULL
) ENGINE=InnoDB;

CREATE INDEX IF NOT EXISTS idx_albums_name ON albums (name);
CREATE INDEX IF NOT EXISTS idx_albums_release_date ON albums (release_date);

CREATE TABLE IF NOT EXISTS tracks (
  track_id         BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  spotify_track_id VARCHAR(22) NOT NULL UNIQUE,
  name             VARCHAR(255) NOT NULL,
  explicit         BOOLEAN,
  duration_min     DECIMAL(6,3),
  disc_number      SMALLINT,
  track_number     SMALLINT,
  isrc             VARCHAR(15) UNIQUE,
  created_at       DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at       DATETIME NULL
) ENGINE=InnoDB;

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
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS album_artists (
  album_id     BIGINT NOT NULL,
  artist_id    BIGINT NOT NULL,
  artist_order SMALLINT,
  PRIMARY KEY (album_id, artist_id),
  CONSTRAINT fk_album_artists_album
    FOREIGN KEY (album_id)  REFERENCES albums  (album_id)  ON DELETE CASCADE,
  CONSTRAINT fk_album_artists_artist
    FOREIGN KEY (artist_id) REFERENCES artists (artist_id) ON DELETE CASCADE
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS track_artists (
  track_id     BIGINT NOT NULL,
  artist_id    BIGINT NOT NULL,
  artist_order SMALLINT,
  PRIMARY KEY (track_id, artist_id),
  CONSTRAINT fk_track_artists_track
    FOREIGN KEY (track_id)  REFERENCES tracks  (track_id)  ON DELETE CASCADE,
  CONSTRAINT fk_track_artists_artist
    FOREIGN KEY (artist_id) REFERENCES artists (artist_id) ON DELETE CASCADE
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS track_albums (
  track_id   BIGINT NOT NULL,
  album_id   BIGINT NOT NULL,
  is_primary BOOLEAN NOT NULL DEFAULT TRUE,
  PRIMARY KEY (track_id, album_id),
  CONSTRAINT fk_track_albums_track
    FOREIGN KEY (track_id) REFERENCES tracks (track_id) ON DELETE CASCADE,
  CONSTRAINT fk_track_albums_album
    FOREIGN KEY (album_id) REFERENCES albums (album_id) ON DELETE CASCADE
) ENGINE=InnoDB;

-- --------------------
-- AUDIO FEATURES (1:1)
-- --------------------

CREATE TABLE IF NOT EXISTS audio_features (
  track_id          BIGINT PRIMARY KEY,
  danceability      DECIMAL(4,3),
  energy            DECIMAL(4,3),
  `key`             SMALLINT,
  mode              SMALLINT,
  loudness          DECIMAL(6,3),
  speechiness       DECIMAL(4,3),
  acousticness      DECIMAL(4,3),
  instrumentalness  DECIMAL(6,5),
  liveness          DECIMAL(4,3),
  valence           DECIMAL(4,3),
  tempo             DECIMAL(6,2),
  time_signature    SMALLINT,
  CONSTRAINT fk_audio_features_track
    FOREIGN KEY (track_id) REFERENCES tracks (track_id) ON DELETE CASCADE
) ENGINE=InnoDB;

-- --------------------
-- CHARTS
-- --------------------

CREATE TABLE IF NOT EXISTS charts (
  chart_id   BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  provider   VARCHAR(40) NOT NULL DEFAULT 'spotify',
  name       VARCHAR(120) NOT NULL,
  chart_type VARCHAR(40),
  market_id  BIGINT,
  CONSTRAINT fk_charts_market
    FOREIGN KEY (market_id) REFERENCES markets (market_id) ON DELETE SET NULL,
  CONSTRAINT uq_charts_unique UNIQUE (provider, name, market_id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS chart_entries (
  chart_entry_id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
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
) ENGINE=InnoDB;

CREATE INDEX IF NOT EXISTS idx_chart_entries_chart_date
  ON chart_entries (chart_id, chart_date);

CREATE INDEX IF NOT EXISTS idx_chart_entries_track_date
  ON chart_entries (track_id, chart_date);

COMMIT;

