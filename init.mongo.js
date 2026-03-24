// Init schema-like setup for MongoDB (runs on first init, safe for manual reruns).
const spotifyDb = db.getSiblingDB("spotify");

function ensureSpotifyAppUser() {
  // App user in spotify DB lets tools authenticate with authSource=spotify.
  const appUser = "user";
  const appPassword = "user";
  try {
    spotifyDb.createUser({
      user: appUser,
      pwd: appPassword,
      roles: [
        { role: "readWrite", db: "spotify" },
        { role: "dbAdmin", db: "spotify" }
      ]
    });
  } catch (e) {
    if (e.codeName !== "DuplicateKey") {
      throw e;
    }
    spotifyDb.updateUser(appUser, {
      pwd: appPassword,
      roles: [
        { role: "readWrite", db: "spotify" },
        { role: "dbAdmin", db: "spotify" }
      ]
    });
  }
}

function ensureCollection(name, validator) {
  try {
    spotifyDb.createCollection(name, {
      validator: validator,
      validationLevel: "moderate"
    });
  } catch (e) {
    if (e.codeName !== "NamespaceExists") {
      throw e;
    }
    spotifyDb.runCommand({
      collMod: name,
      validator: validator,
      validationLevel: "moderate"
    });
  }
}

const schemas = {
  genres: {
    $jsonSchema: {
      bsonType: "object",
      required: ["name"],
      properties: {
        genre_id: { bsonType: ["int", "long", "double", "decimal"] },
        name: { bsonType: "string", minLength: 1 },
        created_at: { bsonType: "date" }
      }
    }
  },
  markets: {
    $jsonSchema: {
      bsonType: "object",
      required: ["name"],
      properties: {
        market_id: { bsonType: ["int", "long", "double", "decimal"] },
        country_code: { bsonType: "string", minLength: 2, maxLength: 2 },
        name: { bsonType: "string", minLength: 1 }
      }
    }
  },
  artists: {
    $jsonSchema: {
      bsonType: "object",
      required: ["name"],
      properties: {
        artist_id: { bsonType: ["int", "long", "double", "decimal"] },
        name: { bsonType: "string", minLength: 1 },
        raw_genres_text: { bsonType: "string" },
        created_at: { bsonType: "date" },
        updated_at: { bsonType: "date" }
      }
    }
  },
  albums: {
    $jsonSchema: {
      bsonType: "object",
      required: ["name"],
      properties: {
        album_id: { bsonType: ["int", "long", "double", "decimal"] },
        spotify_album_id: { bsonType: "string", minLength: 1 },
        name: { bsonType: "string", minLength: 1 },
        album_type: { bsonType: "string" },
        release_date: { bsonType: "date" },
        total_tracks: { bsonType: ["int", "long", "double", "decimal"] },
        created_at: { bsonType: "date" },
        updated_at: { bsonType: "date" }
      }
    }
  },
  tracks: {
    $jsonSchema: {
      bsonType: "object",
      required: ["spotify_track_id", "name"],
      properties: {
        track_id: { bsonType: ["int", "long", "double", "decimal"] },
        spotify_track_id: { bsonType: "string", minLength: 1 },
        name: { bsonType: "string", minLength: 1 },
        explicit: { bsonType: "bool" },
        duration_min: { bsonType: ["int", "long", "double", "decimal"] },
        disc_number: { bsonType: ["int", "long", "double", "decimal"] },
        track_number: { bsonType: ["int", "long", "double", "decimal"] },
        isrc: { bsonType: "string" },
        created_at: { bsonType: "date" },
        updated_at: { bsonType: "date" }
      }
    }
  },
  artist_genres: {
    $jsonSchema: {
      bsonType: "object",
      required: ["artist_id", "genre_id"],
      properties: {
        artist_id: { bsonType: ["int", "long", "double", "decimal"] },
        genre_id: { bsonType: ["int", "long", "double", "decimal"] }
      }
    }
  },
  album_artists: {
    $jsonSchema: {
      bsonType: "object",
      required: ["album_id", "artist_id"],
      properties: {
        album_id: { bsonType: ["int", "long", "double", "decimal"] },
        artist_id: { bsonType: ["int", "long", "double", "decimal"] },
        artist_order: { bsonType: ["int", "long", "double", "decimal"] }
      }
    }
  },
  track_artists: {
    $jsonSchema: {
      bsonType: "object",
      required: ["track_id", "artist_id"],
      properties: {
        track_id: { bsonType: ["int", "long", "double", "decimal"] },
        artist_id: { bsonType: ["int", "long", "double", "decimal"] },
        artist_order: { bsonType: ["int", "long", "double", "decimal"] }
      }
    }
  },
  track_albums: {
    $jsonSchema: {
      bsonType: "object",
      required: ["track_id", "album_id"],
      properties: {
        track_id: { bsonType: ["int", "long", "double", "decimal"] },
        album_id: { bsonType: ["int", "long", "double", "decimal"] },
        is_primary: { bsonType: "bool" }
      }
    }
  },
  audio_features: {
    $jsonSchema: {
      bsonType: "object",
      required: ["track_id"],
      properties: {
        track_id: { bsonType: ["int", "long", "double", "decimal"] },
        danceability: { bsonType: ["int", "long", "double", "decimal"] },
        energy: { bsonType: ["int", "long", "double", "decimal"] },
        key: { bsonType: ["int", "long", "double", "decimal"] },
        mode: { bsonType: ["int", "long", "double", "decimal"] },
        loudness: { bsonType: ["int", "long", "double", "decimal"] },
        speechiness: { bsonType: ["int", "long", "double", "decimal"] },
        acousticness: { bsonType: ["int", "long", "double", "decimal"] },
        instrumentalness: { bsonType: ["int", "long", "double", "decimal"] },
        liveness: { bsonType: ["int", "long", "double", "decimal"] },
        valence: { bsonType: ["int", "long", "double", "decimal"] },
        tempo: { bsonType: ["int", "long", "double", "decimal"] },
        time_signature: { bsonType: ["int", "long", "double", "decimal"] }
      }
    }
  },
  charts: {
    $jsonSchema: {
      bsonType: "object",
      required: ["provider", "name"],
      properties: {
        chart_id: { bsonType: ["int", "long", "double", "decimal"] },
        provider: { bsonType: "string", minLength: 1 },
        name: { bsonType: "string", minLength: 1 },
        chart_type: { bsonType: "string" },
        market_id: { bsonType: ["int", "long", "double", "decimal"] }
      }
    }
  },
  chart_entries: {
    $and: [
      {
        $jsonSchema: {
          bsonType: "object",
          required: ["chart_id", "track_id", "chart_date", "position"],
          properties: {
            chart_entry_id: { bsonType: ["int", "long", "double", "decimal"] },
            chart_id: { bsonType: ["int", "long", "double", "decimal"] },
            track_id: { bsonType: ["int", "long", "double", "decimal"] },
            chart_date: { bsonType: "date" },
            position: { bsonType: ["int", "long", "double", "decimal"] },
            streams: { bsonType: ["int", "long", "double", "decimal"] }
          }
        }
      },
      {
        $or: [
          { position: { $exists: false } },
          { position: { $gt: 0 } }
        ]
      },
      {
        $or: [
          { streams: { $exists: false } },
          { streams: { $gte: 0 } }
        ]
      }
    ]
  }
};

Object.keys(schemas).forEach(function (name) {
  ensureCollection(name, schemas[name]);
});

ensureSpotifyAppUser();

spotifyDb.genres.createIndex({ name: 1 }, { unique: true });
spotifyDb.markets.createIndex(
  { country_code: 1 },
  { unique: true, partialFilterExpression: { country_code: { $type: "string" } } }
);

spotifyDb.artists.createIndex({ name: 1 });

spotifyDb.albums.createIndex(
  { spotify_album_id: 1 },
  { unique: true, partialFilterExpression: { spotify_album_id: { $type: "string" } } }
);
spotifyDb.albums.createIndex({ name: 1 });
spotifyDb.albums.createIndex({ release_date: 1 });

spotifyDb.tracks.createIndex({ spotify_track_id: 1 }, { unique: true });
spotifyDb.tracks.createIndex(
  { isrc: 1 },
  { unique: true, partialFilterExpression: { isrc: { $type: "string" } } }
);
spotifyDb.tracks.createIndex({ name: 1 });

spotifyDb.artist_genres.createIndex({ artist_id: 1, genre_id: 1 }, { unique: true });
spotifyDb.album_artists.createIndex({ album_id: 1, artist_id: 1 }, { unique: true });
spotifyDb.track_artists.createIndex({ track_id: 1, artist_id: 1 }, { unique: true });
spotifyDb.track_albums.createIndex({ track_id: 1, album_id: 1 }, { unique: true });
spotifyDb.audio_features.createIndex({ track_id: 1 }, { unique: true });

spotifyDb.charts.createIndex(
  { provider: 1, name: 1, market_id: 1 },
  {
    unique: true,
    partialFilterExpression: {
      market_id: { $type: ["int", "long", "double", "decimal"] }
    }
  }
);

spotifyDb.chart_entries.createIndex({ chart_id: 1, chart_date: 1 });
spotifyDb.chart_entries.createIndex({ track_id: 1, chart_date: 1 });
spotifyDb.chart_entries.createIndex(
  { chart_id: 1, track_id: 1, chart_date: 1 },
  { unique: true }
);

print("MongoDB init completed for database: spotify");

