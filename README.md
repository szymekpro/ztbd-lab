## Useful actions

> Tip (Windows): run commands with the venv activated (`env/Scripts/Activate.ps1`) or prefix with `./env/Scripts/python.exe`
> to make sure dependencies are available.

### Run data generator postgres
 ```python import_spotify_kaggle.py --csv "spotify_data_clean.csv"```

### Run seeding for postgres
```py seed_psql_faker_data.py --truncate --genres 20 --artists 50 --albums 80 --tracks 1000000 --seed 1```

### Run INSERT benchmark scenarios for postgres (single/complex/bulk/heavy/concurrent/upsert)
```py postgres/benchmark_psql_insert_scenarios.py --scales 500000,1000000,10000000 --both-index-modes```

### Run DELETE benchmark for postgres with and without indexes
```py postgres/benchmark_psql_delete_scenarios.py --scales 500000,1000000,10000000 --both-index-modes```

### Run READ benchmark for postgres with and without indexes
```py postgres/benchmark_psql_read_scenarios.py --scales 500000,1000000,10000000 --both-index-modes```

### Run UPDATE benchmark for postgres with and without indexes
```py postgres/benchmark_psql_update_scenarios.py --scales 500000,1000000,10000000 --both-index-modes```

### Run ALL postgres benchmarks (INSERT + READ + UPDATE + DELETE)
```py postgres/run_all_psql_benchmarks.py --scales 500000,1000000,10000000 --both-index-modes```

### Run ALL cassandra benchmarks (INSERT + READ + UPDATE + DELETE)
```py cassandra/run_all_cassandra_benchmarks.py --scales 500000,1000000,10000000 --both-index-modes --seed 1```

### Run ALL mariadb benchmarks (INSERT + READ + UPDATE + DELETE)
```py mariadb/run_all_mariadb_benchmarks.py --scales 500000,1000000,10000000 --both-index-modes --seed 1```

### Run ALL mongodb benchmarks (INSERT + READ + UPDATE + DELETE)
```py mongodb/run_all_mongodb_benchmarks.py --scales 500000,1000000,10000000 --both-index-modes --seed 1```

### Run ALL benchmarks (ALL databases)
This runs Cassandra + MariaDB + PostgreSQL + MongoDB sequentially and forwards: `--scales`, `--both-index-modes`, `--seed`.

```py run_all_benchmarks.py --scales 500000,1000000,10000000 --both-index-modes --seed 1```

### Run seeding for mariadb
```py seed_mariadb_faker_data.py --truncate --genres 20 --artists 50 --albums 80 --tracks 1000000 --seed 1```

### Run seeding for cassandra
```py seed_cassandra_faker_data.py --truncate --genres 20 --artists 50 --albums 80 --tracks 1000000 --seed 1```

### Faster seeding for cassandra (large loads)
```py seed_cassandra_faker_data.py --truncate --genres 20 --artists 50 --albums 80 --tracks 1000000 --seed 1 --write-concurrency 300 --write-chunk-size 20000 --progress-every 200000```

### Fast mode (quick stress dataset)
```py seed_cassandra_faker_data.py --truncate --genres 20 --artists 50 --albums 80 --tracks 1000000 --seed 1 --fast```

### Plot results
```py visualization/plot_results.py --results-dir .\postgres\results --output-dir .\visualization\charts```