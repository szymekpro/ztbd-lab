### Wymagnia:

- Porównanie wyników testów przed i po zastosowaniu indeksów. 
- Średnią z 3 prób dla każdej operacji CRUD 
(minimum 3 próby dla każdego z 24 scenariuszy testowych). 
- Rozszerzoną analizę wyników oraz wniosków. 
- Co najmniej 24 różne scenariusze testowe, 
w tym minimum 6 scenariuszy dla każdej operacji CRUD. 

### Scenariusze:

Pojedynczy wstawienie (Single Insert): Utworzenie nowego artysty w tabeli artists z przypisaniem podstawowych danych (name, raw_genres_text).

    Wstawienie powiązane (Complex Insert): Dodanie nowego utworu (tracks) od razu z jego parametrami audio (audio_features) oraz powiązaniem z albumem i artystą. Testuje wydajność transakcji (SQL) vs zapis denormalizowanego dokumentu (NoSQL).

    Zapis masowy (Bulk Insert / Batching): Dzienny import 10 000 nowych notowań do tabeli chart_entries. Testujemy tu zoptymalizowane komendy masowe (COPY w Postgresie, insertMany w Mongo, BATCH w Cassandrze).

    Wstawienie dużego ładunku (Heavy Payload): Zapis rekordu artysty (artists), w którym pole raw_genres_text zostaje sztucznie obciążone bardzo długim ciągiem znaków (np. 50 KB JSON z metadanymi).

    Współbieżne wstawianie (Concurrent Inserts): Uruchomienie 100-500 równoległych wątków aplikacji, z których każdy dodaje pojedyncze odsłuchy (nowe rekordy do tabel podobnych do chart_entries).

    Operacja Upsert (Insert or Update): Aktualizacja dziennego notowania w chart_entries. Jeśli utwór wpada na listę pierwszy raz – dodajemy go; jeśli już tam jest danego dnia – aktualizujemy jego streams i position.

2. Scenariusze Testowe: READ (Odczyt)

Testy odczytu weryfikują skuteczność indeksowania, użycia pamięci RAM (Cache) oraz mechanizmów filtrowania silników.

    Odczyt punktowy (Point Read): Pobranie dokładnych cech audio (audio_features) dla znanego identyfikatora track_id lub spotify_track_id. Najszybsza operacja, testująca czyste opóźnienie (latency).

    Odczyt po kluczu obcym/partycji (Partition Read): Pobranie pełnej listy utworów (tracks) dla konkretnego albumu (album_id). W SQL wymaga JOIN z track_albums, w NoSQL – odczytu z partycji lub zagnieżdżonej tablicy.

    Top-N / Paginacja (Top-N Ranking): Pobranie Top 50 najwyższych pozycji z chart_entries dla wybranego rynku (market_id) i konkretnej daty (chart_date), posortowanych po position rosnąco.

    Odczyt po atrybucie drugorzędnym (Secondary Index): Wyszukanie wszystkich utworów, w których flaga explicit = true. To testuje wydajność skanowania indeksów (Secondary Indexes).

    Agregacja lokalna (Local Aggregation): Obliczenie średniego tempa (tempo) i taneczności (danceability) z tabeli audio_features dla wszystkich utworów powiązanych z jednym, konkretnym artystą.

    Skanowanie zakresowe (Range Query / Full Scan): Pobranie wszystkich albumów (albums), których release_date przypada na okres między 2015 a 2020 rokiem.

3. Scenariusze Testowe: UPDATE (Aktualizacja)

Testy te uwydatniają różnice w zarządzaniu blokadami (locking) oraz mutowalnością struktur na dysku (w-place updates w SQL vs append-only w Cassandrze).

    Aktualizacja pojedynczego pola (Point Update): Korekta literówki w nazwie utworu (name w tracks) na podstawie jego głównego track_id.

    Aktualizacja zagnieżdżona/1:1 (Nested Update): Zmiana wartości energy dla konkretnego utworu. (W bazach relacyjnych to operacja na powiązanej tabeli audio_features, w Mongo edycja zagnieżdżonego pola dokumentu).

    Aktualizacja masowa (Bulk Update): Ustawienie pola explicit = true dla wszystkich utworów należących do wybranego gatunku muzycznego.

    Inkrementacja licznika (Atomic Increment): Zwiększenie liczby odtworzeń (streams) w tabeli chart_entries dla konkretnego utworu i dnia o 1000.

    Dopisanie do kolekcji/relacji (List Append): Przypisanie nowego, dodatkowego gatunku muzycznego (rekordu w genres) do istniejącego artysty (poprzez modyfikację artist_genres).

    Aktualizacja warunkowa (CAS - Compare and Set): Zmiana position w chart_entries dla utworu tylko i wyłącznie wtedy, gdy nowa pozycja jest niższa (lepsza) niż ta obecnie zapisana w bazie (zapobieganie wyścigom).

4. Scenariusze Testowe: DELETE (Usuwanie)

Usuwanie danych to często najsłabszy punkt wielu systemów, wymagający zwalniania miejsca, aktualizacji indeksów drzewiastych (SQL) lub zarządzania "Tombstones" (Cassandra).

    Pojedyncze usunięcie (Point Delete): Całkowite usunięcie pojedynczego rekordu z tabeli słownikowej, np. jednego rynku z tabeli markets (zakładając brak ograniczeń klucza obcego w teście).

    Usunięcie kaskadowe/partycji (Cascade Delete): Skasowanie całego albumu (albums), co w SQL wymusza automatyczne wyzwolenie ON DELETE CASCADE i usunięcie relacji w track_albums oraz album_artists.

    Usunięcie relacji (Relationship Delete): Usunięcie artysty-współtwórcy z danego utworu (skasowanie pojedynczego rekordu z track_artists / usunięcie elementu z tablicy w dokumencie MongoDB).

    Usuwanie zakresowe / przeterminowanie (Range / TTL Delete): Cykliczne "czyszczenie bazy" polegające na skasowaniu wszystkich notowań (chart_entries), których chart_date jest starsze niż 3 lata.

    Współbieżne usuwanie (Concurrent Deletes): Uruchomienie kilkudziesięciu wątków losowo kasujących wybrane utwory (tracks), aby sprawdzić mechanizmy rozwiązywania konfliktów (Deadlocks).

    Miękkie usuwanie (Soft Delete): Zamiast twardego DELETE, wykonujemy UPDATE albums SET updated_at = now() i symulujemy "ukrycie" albumu. Testujemy różnicę kosztów wejścia/wyjścia względem faktycznego skasowania rekordu.