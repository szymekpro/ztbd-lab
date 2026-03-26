## Useful actions

### Run data generator postgres
 ```python import_spotify_kaggle.py --csv "spotify_data_clean.csv"```

### Run seeding for postgres
```py seed_psql_faker_data.py --truncate --genres 20 --artists 50 --albums 80 --tracks 1000000 --seed 1```

### Run seeding for mariadb
```py seed_mariadb_faker_data.py --truncate --genres 20 --artists 50 --albums 80 --tracks 1000000 --seed 1```