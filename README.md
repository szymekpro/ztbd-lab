## Useful actions

### Run data generator postgres
 ```python import_spotify_kaggle.py --csv "spotify_data_clean.csv"```

### Run seeding for postgres
```py seed_psql_faker_data.py --truncate --genres 20 --artists 50 --albums 80 --tracks 1000000 --seed 1```

### Run INSERT benchmark scenarios for postgres (single/complex/bulk/heavy/concurrent/upsert)
```py benchmark_psql_insert_scenarios.py --scales 500000,1000000,10000000 --runs-per-scenario 3 --prepare-mode seed-script --seed-value 1```

### Run DELETE benchmark for postgres with and without indexes
```py postgres/benchmark_psql_delete_scenarios.py --scales 500000,1000000,10000000 --both-index-modes```

### Run seeding for mariadb
```py seed_mariadb_faker_data.py --truncate --genres 20 --artists 50 --albums 80 --tracks 1000000 --seed 1```

### Run seeding for cassandra
```py seed_cassandra_faker_data.py --truncate --genres 20 --artists 50 --albums 80 --tracks 1000000 --seed 1```

### Faster seeding for cassandra (large loads)
```py seed_cassandra_faker_data.py --truncate --genres 20 --artists 50 --albums 80 --tracks 1000000 --seed 1 --write-concurrency 300 --write-chunk-size 20000 --progress-every 200000```

### Fast mode (quick stress dataset)
```py seed_cassandra_faker_data.py --truncate --genres 20 --artists 50 --albums 80 --tracks 1000000 --seed 1 --fast```

### Plot results
```py plot_results.py --results-dir .\postgres\results --output-dir .\visualization\charts```