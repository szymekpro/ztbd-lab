# Bazki - Postgres + MariaDB + MongoDB + Cassandra

## Start baz

```powershell
docker compose up -d db mariadb mongodb
docker compose up -d cassandra cassandra-init
```

## Init i wolumeny

- `init.postgres.sql` uruchamia sie przy pierwszej inicjalizacji wolumenu `pgdata`.
- `init.mariadb.sql` uruchamia sie przy pierwszej inicjalizacji wolumenu `mariadb_data`.
- `init.mongo.js` uruchamia sie przy pierwszej inicjalizacji wolumenu `mongodb_data`.
- `init.cassandra.cql` uruchamia serwis `cassandra-init` po starcie `cassandra`.

Jesli zmienisz plik init i chcesz uruchomic go od zera, zresetuj odpowiedni wolumen.

```powershell
docker compose down
docker volume rm bazki_pgdata
docker volume rm bazki_mariadb_data
docker volume rm bazki_mongodb_data
docker volume rm bazki_cassandra_data
docker compose up -d db mariadb mongodb cassandra cassandra-init
```

## Szybka weryfikacja

```powershell
docker exec spotify-postgres psql -U user -d user -c "\dt spotify.*"
docker exec spotify-mariadb mariadb -uuser -puser -e "USE spotify; SHOW TABLES;"
docker exec spotify-mongodb mongosh -u spotify -p spotify --authenticationDatabase admin --eval "db.getSiblingDB('spotify').getCollectionNames()"
docker exec spotify-cassandra cqlsh -e "DESCRIBE KEYSPACES"
docker exec spotify-cassandra cqlsh -e "USE spotify; DESCRIBE TABLES"
```

## Logowanie do MongoDB

- Admin (`spotify` / `spotify` z `docker-compose.yml`) loguje sie przez `authSource=admin`.
- Uzytkownik aplikacyjny: `user` / `user` loguje sie do `spotify` (`authSource=spotify`).

Przyklad URI:

```text
mongodb://spotify:spotify@localhost:27018/spotify?authSource=admin
mongodb://user:user@localhost:27018/spotify?authSource=spotify
```

## Import CSV (PostgreSQL)

```powershell
python import_spotify_kaggle.py --csv "spotify_data_clean.csv"
```
