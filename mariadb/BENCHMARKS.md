# MariaDB vs PostgreSQL — porównanie benchmarków

Dokument opisuje, w jakim stopniu scenariusze benchmarkowe MariaDB odzwierciedlają te z PostgreSQL oraz jakie różnice (celowe i wynikające z architektury) pozostają.

---

## Różnice architektoniczne

| Cecha | PostgreSQL | MariaDB |
|---|---|---|
| Driver | `psycopg` | `PyMySQL` |
| Bulk insert | `COPY … FROM STDIN` / `generate_series` | `LOAD DATA LOCAL INFILE` / `executemany` |
| Upsert | `INSERT … ON CONFLICT DO UPDATE` | `INSERT … ON DUPLICATE KEY UPDATE` |
| Auto-increment PK | `BIGSERIAL` / `GENERATED ALWAYS` | `BIGINT AUTO_INCREMENT` |
| Transakcje | `conn.transaction()` (context manager) | `conn.commit()` / `conn.rollback()` |
| Subquery UPDATE | `UPDATE … FROM (SELECT …)` | `UPDATE … WHERE id IN (SELECT … FROM (…) x)` — podwójny alias z powodu ograniczenia MariaDB |
| Operacje warunkowe | `UPDATE … WHERE … RETURNING` | `UPDATE … WHERE …` + oddzielny `SELECT` |
| Indeksy schematu | FK automatycznie nie tworzy indeksu | FK automatycznie nie tworzy indeksu (InnoDB) |
| `DROP INDEX` | `DROP INDEX IF EXISTS idx` | `DROP INDEX IF EXISTS idx ON table` |

---

## Seeder — wyrównanie z PostgreSQL

`seed_mariadb_faker_data.py` jest funkcjonalnie identyczny z `seed_psql_faker_data.py` we wszystkich kluczowych aspektach:

- Te same proporcje danych: artyści, albumy, tracki, gatunki, rynki, chart_entries
- Deterministyczne ID: `spotify_id_from_int`, `isrc_from_int` — ten sam algorytm Base62/Base36
- Relacje przez modulo arytmetyczne (`INSERT … SELECT … MOD(…)`)
- `audio_features` generowane przez deterministyczne haszowanie `track_id * 1103515245`
- Parametr `include_audio_features` (dodany w ramach alignmentu) — pozwala pominąć seeding `audio_features` dla benchmarków INSERT i DELETE, analogicznie jak w PSQL

Jedyna techniczna różnica: PSQL używa `COPY … FROM STDIN` przez bufory, MariaDB używa `LOAD DATA LOCAL INFILE` przez tymczasowy plik TSV — oba są bulk-load i mają porównywalną wydajność seedowania.

---

## Parametry benchmarku — identyczne

| Parametr | Wartość domyślna |
|---|---|
| `--scales` | `500000,1000000,10000000` |
| `--runs-per-scenario` | `3` |
| `--bulk-size` | `10 000` |
| `--concurrent-workers` | `100` (INSERT), `50` (DELETE) |
| `--concurrent-chunk-size` | `500` |
| `--both-index-modes` | uruchamia bez + z indeksami |

---

## INSERT — 6 scenariuszy

| Scenariusz | PostgreSQL | MariaDB | Różnica |
|---|---|---|---|
| `single_insert` | `INSERT … RETURNING` | `INSERT` + `lastrowid` | Brak RETURNING — ale test mierzy tylko czas insertu, nie odczytu ID |
| `complex_insert` | `INSERT … RETURNING` w jednej transakcji | `lastrowid` w jednej transakcji | Bez RETURNING; ta sama logika atomowości |
| `bulk_insert` | `INSERT … SELECT generate_series(…)` | `executemany` z listą Python | Inny mechanizm, ale ten sam cel: wstaw N rekordów jednym wywołaniem |
| `heavy_payload_insert` | Pętla + CTE, `raw_genres_text` 500 B | Pętla z `INSERT IGNORE` | Identyczna logika, różnica składni |
| `concurrent_inserts` | `ThreadPoolExecutor` + `COPY` per worker | `ThreadPoolExecutor` + `executemany` per worker | Identyczna współbieżność |
| `upsert_insert_or_update` | `ON CONFLICT DO UPDATE` | `ON DUPLICATE KEY UPDATE` | Semantycznie identyczne |

**Seeding INSERT:** `audio_features` **nie są** seedowane (jak w PSQL) — tabela jest pusta podczas testu wstawiania.

**Indeksy zarządzane (INSERT):**
- `idx_albums_release_date` (schematowy, może być usuwany/dodawany)
- `idx_track_albums_album_id` *(nowy — wyrównany z PSQL)*
- `idx_track_artists_artist_id` *(nowy — wyrównany z PSQL)*

---

## READ — 6 scenariuszy

| Scenariusz | PostgreSQL | MariaDB | Różnica |
|---|---|---|---|
| `point_read` | `SELECT … FROM audio_features WHERE track_id = $1` | Identyczne z `%s` | Brak różnic logicznych |
| `partition_read` | `JOIN track_albums WHERE album_id = $1` | Identyczne | Brak różnic |
| `top_n_ranking` | `JOIN tracks … ORDER BY chart_date DESC, position LIMIT N` | Identyczne | Brak różnic |
| `secondary_index_read` | `WHERE explicit = true LIMIT N` | Identyczne | Brak różnic |
| `local_aggregation` | `JOIN track_artists … GROUP BY` (server-side) | Identyczne | Brak różnic — SQL w pełni obsługuje agregacje |
| `range_query` | `WHERE release_date BETWEEN … ORDER BY release_date LIMIT N` | Identyczne | Brak różnic |

**Seeding READ:** `audio_features` **są** seedowane (jak w PSQL).

**Indeksy zarządzane (READ):**
- `idx_albums_release_date`, `idx_chart_entries_chart_date`, `idx_chart_entries_track_date` (schematowe)
- `idx_track_albums_album_id` *(nowy — wyrównany z PSQL)*
- `idx_track_artists_artist_id` *(nowy — wyrównany z PSQL)*
- `idx_tracks_explicit` *(nowy)*

READ jest scenariuszem z najlepszym alignmentem — SQL jest praktycznie 1:1.

---

## UPDATE — 6 scenariuszy

| Scenariusz | PostgreSQL | MariaDB | Różnica |
|---|---|---|---|
| `point_update` | `UPDATE … WHERE track_id = ANY($1::bigint[])` | `UPDATE … WHERE track_id IN (%s, %s, …)` | MariaDB nie obsługuje `ANY(array)` — odpowiednik `IN (...)` jest semantycznie identyczny |
| `nested_update` | `UPDATE audio_features SET energy = $1 WHERE track_id = $2` | Identyczne | Brak różnic |
| `bulk_update` | `UPDATE … WHERE track_id IN (SELECT … JOIN …)` | `UPDATE … WHERE track_id IN (SELECT … FROM (SELECT …) x)` | Podwójny alias (obejście MariaDB) — ta sama logika |
| `atomic_increment` | `UPDATE … SET streams = streams + 1000 … LIMIT N` | Identyczne z `ORDER BY … LIMIT` | Brak różnic semantycznych |
| `list_append` | `INSERT … ON CONFLICT DO NOTHING` | `INSERT IGNORE INTO …` | Semantycznie identyczne |
| `cas_update` | `UPDATE … WHERE … AND position > $3` | Identyczne | Brak różnic — warunkowy UPDATE |

**Seeding UPDATE:** `audio_features` **są** seedowane (jak w PSQL).

**Indeksy zarządzane (UPDATE):**
- `idx_chart_entries_track_date` (schematowy)
- `idx_artist_genres_artist_id` *(nowy — wyrównany z PSQL)*
- `idx_track_artists_artist_id` *(nowy — wyrównany z PSQL)*
- `idx_tracks_explicit` *(nowy)*

---

## DELETE — 6 scenariuszy

| Scenariusz | PostgreSQL | MariaDB | Różnica |
|---|---|---|---|
| `point_delete` | `DELETE FROM markets WHERE market_id = $1` | Identyczne | Brak różnic |
| `cascade_delete` | `DELETE FROM albums WHERE album_id = $1` (FK CASCADE) | Identyczne — MariaDB InnoDB obsługuje FK CASCADE | Brak różnic |
| `relationship_delete` | `DELETE FROM track_artists … JOIN tracks …` | `DELETE ta FROM track_artists ta JOIN tracks t …` | Składnia JOIN DELETE różni się (MariaDB wymaga aliasu tabeli przed FROM) — semantycznie identyczne |
| `range_delete` | `DELETE WHERE chart_id = $1 AND chart_date < $2` | Identyczne | Brak różnic |
| `concurrent_delete` | `ThreadPoolExecutor` + `DELETE WHERE track_id IN (…)` | Identyczne | Brak różnic |
| `soft_delete` | `UPDATE albums SET updated_at = NOW() WHERE album_id = $1` | Identyczne | Brak różnic |

**Seeding DELETE:** `audio_features` **nie są** seedowane (jak w PSQL) — szybszy reseed dla dużych skal.

**Indeksy zarządzane (DELETE):**
- `idx_chart_entries_chart_date`, `idx_albums_release_date` (schematowe)
- `idx_track_albums_album_id` *(nowy — wyrównany z PSQL)*
- `idx_track_artists_artist_id` *(nowy — wyrównany z PSQL)*

---

## Podsumowanie alignmentu

| Obszar | Status | Opis |
|---|---|---|
| Seeder — dane | ✅ 1:1 | Identyczne proporcje, algorytmy ID, relacje |
| Seeder — audio_features | ✅ wyrównany | Parametr `include_audio_features` dodany |
| INSERT scenariusze | ✅ 1:1 (z różnicami składni) | `RETURNING` → `lastrowid`; `generate_series` → `executemany` |
| READ scenariusze | ✅ 1:1 | SQL praktycznie identyczny |
| UPDATE scenariusze | ✅ 1:1 (z różnicami składni) | `ANY(array)` → `IN(…)`; podwójny alias subquery |
| DELETE scenariusze | ✅ 1:1 (z różnicami składni) | `DELETE … USING` → `DELETE alias FROM … JOIN` |
| Indeksy zarządzane | ✅ wyrównane | Dodano brakujące `idx_track_albums_album_id`, `idx_track_artists_artist_id`, `idx_artist_genres_artist_id` |
| Współbieżność | ✅ 1:1 | `ThreadPoolExecutor` identycznie skonfigurowany |

Różnice, które **celowo pozostają** (wynikają z architektury silnika):
- `COPY FROM STDIN` (PSQL) vs `LOAD DATA LOCAL INFILE` (MariaDB) — bulk load, oba szybkie
- `ON CONFLICT DO UPDATE` vs `ON DUPLICATE KEY UPDATE` — semantycznie identyczne
- `DELETE … USING` vs `DELETE alias FROM … JOIN` — semantycznie identyczne
- Brak `RETURNING` w MariaDB — używamy `lastrowid`; nie wpływa na mierzony czas insertu

---

## Przydatne komendy

```bash
# Uruchomienie MariaDB przez Docker
docker-compose up -d mariadb

# INSERT benchmark (skala 500k, oba tryby indeksów)
cd mariadb
python benchmark_mariadb_insert_scenarios.py --scales 500000 --both-index-modes --runs-per-scenario 3

# READ benchmark
python benchmark_mariadb_read_scenarios.py --scales 500000,1000000 --both-index-modes

# UPDATE benchmark
python benchmark_mariadb_update_scenarios.py --scales 500000 --both-index-modes

# DELETE benchmark (z reseedowaniem per tryb)
python benchmark_mariadb_delete_scenarios.py --scales 500000 --both-index-modes --reseed-per-index-mode

# Pomiń seedowanie (zakładając gotową bazę)
python benchmark_mariadb_insert_scenarios.py --scales 500000 --skip-prepare
```
