# BScThesis

## Todd's BSc research repository

    git clone git@github.com:YeOldeOak/BScThesis

## Installing DuckDB

### v1.1.3 bugfix release

    wget https://github.com/duckdb/duckdb/releases/download/v1.1.3/duckdb_cli-linux-amd64.zip
    unzip duckdb_cli-linux-amd64.zip

### DuckDB nightly

    wget https://artifacts.duckdb.org/latest/duckdb-binaries-linux.zip
    unzip duckdb-binaries-linux.zip 

### Build from source

Nightly may not work out well if extensions were not uploaded.

Checkout the DuckDB repo, switch to `feature` and `DUCKDB_EXTENSIONS=httpfs make -j 4`.

## Using DuckDB

Original queries for dictionary and ranking by BM25:

    duckdb --init init.sql todd.db < todd-schema.sql
    duckdb --init init.sql todd.db < todd-queries.sql

When using a database file (`todd.db`), the first command defining the views is only needed once.

Experiments to trigger different physical designs:

    duckdb --init init.sql todd.db < schema.sql
    duckdb --init init.sql todd.db < queries.sql

