# Benchmarki Cassandra vs PostgreSQL

Dokumentacja porównuje jak te same scenariusze są realizowane w Cassandrze i PostgreSQL.
Celem jest uczciwe porównanie wydajności przy identycznym obciążeniu logicznym.

---

## Różnice architektoniczne (nieusuwalne)

Cassandra i PostgreSQL różnią się fundamentalnie — niektóre różnice w implementacji scenariuszy
wynikają z ograniczeń silnika, nie z błędów w benchmarku:

| Aspekt | Cassandra | PostgreSQL |
|--------|-----------|-----------|
| Klucze obce | brak — relacje zarządzane ręcznie | pełne FK z CASCADE |
| JOIN | niemożliwy | natywny |
| Transakcje | brak ACID (poza LWT) | pełne transakcje ACID |
| Bulk insert | `execute_concurrent_with_args` | `INSERT ... SELECT generate_series` / `COPY` |
| Agregacje server-side | brak `AVG`/`SUM` z JOIN | pełny SQL agregujący |

---

## Przygotowanie danych (seeder)

Oba seedery generują **identyczną strukturę i ilość danych** przy tym samym `--seed`:

| Tabela | Cassandra | PostgreSQL | Zgodność |
|--------|-----------|-----------|---------|
| genres, markets | explicit ID od 1 | serial (IDENTITY) | dane identyczne |
| artists, albums, tracks | 22-znakowy spotify_id z `spotify_id_from_int` | identyczna formuła | ✓ |
| Relacje (modulo) | `((id + gs - 2) % n) + 1` | SQL odpowiednik tej samej formuły | ✓ |
| audio_features | LCG hash `track_id * 1103515245 + X` | identyczna formuła | ✓ |
| chart_entries | 20 chartów × 7 dni × top 50 = 7 000 wierszy | identyczna ilość | ✓ |

Kluczowe: benchmarki INSERT i DELETE **nie seedują audio_features** w obu bazach
(`include_audio_features=False`), READ i UPDATE seedują z `audio_features=True`.

---

## INSERT — porównanie scenariuszy

### S1 — single_insert

Wstawienie jednego tracka.

- **Cassandra** — `INSERT INTO tracks (...) VALUES (...)` z jawnym `track_id` (UUID-like bigint)
- **PostgreSQL** — to samo, ale `track_id` generowane przez schemat (`IDENTITY`), nie przekazywane
- Wynik: mierzy czas jednego `INSERT` w obu bazach — porównywalny

### S2 — complex_insert

Wstawienie artysty + albumu + N tracków z relacjami (`track_albums`, `track_artists`).

- **Cassandra** — sekwencyjne `session.execute()` w pętli; każda operacja to osobny round-trip
- **PostgreSQL** — wszystko w jednej transakcji `RETURNING`; jeden commit
- Różnica: Cassandra nie ma transakcji, każdy INSERT jest od razu trwały

### S3 — bulk_insert

Wstawienie dużej paczki tracków (domyślnie skalowane do 1–200 tys. wierszy).

- **Cassandra** — `execute_concurrent_with_args` z prepared statement; wiele równoległych in-flight operacji
- **PostgreSQL** — jeden `INSERT ... SELECT ... FROM generate_series(1, N)` — cały bulk jako jeden statement
- Różnica celowa: każda baza używa swojego natywnego mechanizmu masowego wstawiania

### S4 — heavy_payload_insert

Wstawienie 250 artystów z dużym `raw_genres_text` + 3 gatunki każdy.

- **Cassandra** — pętla: `INSERT INTO artists` → `INSERT INTO artist_genres` per artysta
- **PostgreSQL** — jeden CTE: `WITH inserted_artists AS (INSERT ... RETURNING) INSERT INTO artist_genres JOIN LATERAL ...`
- Różnica celowa: PostgreSQL używa operacji set-based, Cassandra row-by-row

### S5 — concurrent_inserts

Równoległe wstawienie N tracków z wielu wątków.

- **Cassandra** — `ThreadPoolExecutor`, każdy worker: własna sesja + `execute_concurrent_with_args` per chunk
- **PostgreSQL** — `ThreadPoolExecutor`, każdy worker: jedno połączenie + `INSERT ... generate_series` per chunk
- Podział pracy (buckety, chunk_size): identyczny w obu bazach

### S6 — upsert_insert_or_update

Próba wstawienia tego samego tracka dwa razy.

- **Cassandra** — dwa `INSERT` na ten sam `track_id`; drugi nadpisuje (semantyka upsert z natury Cassandry)
- **PostgreSQL** — `INSERT ... ON CONFLICT (spotify_track_id) DO UPDATE SET ...`
- Wynik: oba rozwiązują konflikt, różnica w mechanizmie

---

## READ — porównanie scenariuszy

### S1 — point_read
`SELECT audio_features WHERE track_id = ?` — identyczny w obu bazach.

### S2 — partition_read
Wszystkie tracki dla albumu.
- **Cassandra** — `SELECT ... FROM track_albums WHERE album_id = ? LIMIT 200`
	- w trybie `with_indexes` zapytanie działa przez indeks wtórny `idx_track_albums_album_id` na `track_albums(album_id)`
	- w trybie `no_indexes` skrypt wykonuje fallback z `ALLOW FILTERING`, żeby scenariusz uruchamiał się w obu trybach
- **PostgreSQL** — `SELECT tracks JOIN track_albums WHERE album_id = ?`
- Cassandra nie może zrobić JOIN, więc zwraca tylko dane z `track_albums`

Uwaga: fallback z `ALLOW FILTERING` zachowuje sens obciążenia (album_id → lista tracków), ale może być wyraźnie wolniejszy
i mniej stabilny na większych skalach, bo Cassandra nie ma naturalnego klucza partycji po `album_id` w tej tabeli.

### S3 — top_n_ranking
Top-N wpisów chartu posortowanych po dacie.
- **Cassandra** — `WHERE chart_id = ? ORDER BY chart_date DESC LIMIT N` (chart_date to clustering key)
- **PostgreSQL** — to samo + JOIN do `tracks` po `t.name`
- Cassandra nie może dołączyć nazwy tracka bez dodatkowego zapytania

### S4 — secondary_index_read
`SELECT tracks WHERE explicit = true LIMIT N` — logicznie identyczny w obu bazach.
Cassandra:
- w trybie `with_indexes` używa indeksu wtórnego na `tracks(explicit)` (bez `ALLOW FILTERING`)
- w trybie `no_indexes` wykonuje fallback z `ALLOW FILTERING`, żeby scenariusz uruchamiał się w obu trybach

### S5 — local_aggregation
Średnie tempo/danceability dla artysty.
- **Cassandra** — N+1 zapytań: najpierw lista tracków artysty, potem `SELECT audio_features` per track, agregacja w Pythonie
- **PostgreSQL** — jeden `AVG(...) ... JOIN track_artists ... JOIN audio_features WHERE artist_id = ?`
- Różnica celowa: mierzy brak agregacji server-side w Cassandrze

Uwaga dot. stabilności w trybie `with_indexes`:
- Indeksy wtórne w Cassandrze po `CREATE INDEX` budują się asynchronicznie na istniejących danych, więc benchmark wykonuje krótki warm-up
	(małe zapytania po indeksach) zanim zacznie mierzyć czasy.

### S6 — range_query
Albumy wydane 2015–2020, LIMIT skalowane.
- **Cassandra** — `WHERE release_date >= ? AND release_date <= ? ALLOW FILTERING LIMIT N`
- **PostgreSQL** — `WHERE release_date BETWEEN ? AND ? ORDER BY release_date LIMIT N`
- PSQL sortuje (release_date jest indeksowane), Cassandra nie może

---

## UPDATE — porównanie scenariuszy

### S1 — point_update (scaled)
Aktualizacja N tracków po PK.
- **Cassandra** — `execute_concurrent_with_args` z prepared `UPDATE ... WHERE track_id = ?` (N równoległych operacji)
- **PostgreSQL** — jeden `UPDATE ... WHERE track_id = ANY(%s)` (array N id-ków)

### S2 — nested_update
`UPDATE audio_features SET energy = ? WHERE track_id = ?` — identyczny logicznie.

### S3 — bulk_update
Ustaw `explicit = true` dla tracków z gatunku.
- **Cassandra** — multi-step: query artystów → query tracków → N `UPDATE` per track
- **PostgreSQL** — jeden `WITH target_tracks AS (...) UPDATE tracks ... FROM target_tracks`

### S4 — atomic_increment
Zwiększ `streams` o 1000 w chart_entries.
- **Cassandra** — read-modify-write: odczyt wierszy, potem `UPDATE SET streams = current + 1000`; **NIE jest atomowy**
- **PostgreSQL** — `UPDATE SET streams = streams + 1000` atomowy server-side
- Różnica celowa: Cassandra nie ma server-side increment na zwykłych kolumnach

### S5 — list_append
Dodanie gatunku do artysty.
- **Cassandra** — `INSERT INTO artist_genres` (silent upsert)
- **PostgreSQL** — `INSERT ... ON CONFLICT DO NOTHING`

### S6 — cas_update
Zmień position tylko gdy nowa wartość jest lepsza.
- **Cassandra** — `UPDATE ... IF position > ?` (LWT — Lightweight Transaction; angażuje quorum)
- **PostgreSQL** — `UPDATE ... WHERE position > ?` (row-level lock, tańsze)
- Różnica celowa: mierzy koszt LWT w Cassandrze

---

## DELETE — porównanie scenariuszy

### S1 — point_delete
`DELETE FROM markets WHERE market_id = ?` — identyczny.

### S2 — cascade_delete
Usunięcie albumu z relacjami.
- **Cassandra** — ręczne: DELETE track_albums → DELETE album_artists → DELETE albums (brak FK)
- **PostgreSQL** — jeden `DELETE FROM albums` + automatyczny FK CASCADE

### S3 — relationship_delete
Usunięcie powiązań track_artists dla paczki tracków.
- **Cassandra** — bulk INSERT przez `execute_concurrent_with_args`, potem `DELETE WHERE track_id + artist_id` per wiersz
- **PostgreSQL** — bulk INSERT przez COPY, potem `DELETE ... USING tracks WHERE spotify_track_id LIKE ?`

### S4 — range_delete
Usunięcie starych chart_entries (> 3 lata).
- **Cassandra** — `DELETE FROM chart_entries WHERE chart_id = ? AND chart_date < ?` (CQL range delete; chart_date to clustering key)
- **PostgreSQL** — `DELETE FROM chart_entries WHERE chart_id = ? AND chart_date < ?`
- Logicznie identyczny — oba wykonują jeden statement

### S5 — concurrent_delete
Równoległe usuwanie tracków z wielu wątków.
- **Cassandra** — `ThreadPoolExecutor`, każdy worker: `execute_concurrent_with_args` na liście track_id
- **PostgreSQL** — `ThreadPoolExecutor`, każdy worker: `DELETE WHERE track_id = ANY(%s)` per chunk

### S6 — soft_delete
`UPDATE albums SET updated_at = now() WHERE album_id = ?` — identyczny.

---

## Metryki (CSV output)

Każdy benchmark zapisuje wyniki do `cassandra/results/cassandra_*_benchmark_results.csv`:

| Kolumna | Opis |
|---------|------|
| `scale` | Liczba tracków w bazie podczas testu |
| `index_mode` | `no_indexes` lub `with_indexes` |
| `scenario` | Nazwa scenariusza |
| `run` | Numer powtórzenia (domyślnie 3 runy) |
| `seconds` | Czas wykonania scenariusza |
| `operations` / `rows_affected` / `rows_returned` | Ilość przetworzonych wierszy |
| `ops_per_sec` / `rows_per_sec` | Przepustowość |

Format CSV jest identyczny z PostgreSQL (`postgres/results/`), co umożliwia bezpośrednie porównanie wyników.
