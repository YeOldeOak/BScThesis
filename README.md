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

Activate VPN and create the correct `s3-private-init.sql` file with your secrets.

### Experiments

We create a database file (`todd.db`) and the view definitions according to Todd's files in S3:

    duckdb --init init.sql todd.db < todd-schema.sql

Run the original queries for dictionary and ranking by BM25 as follows:

    duckdb --init init.sql todd.db < todd-queries.sql

Experiments by Arjen intended to trigger different physical query plans:

    duckdb --init init.sql todd.db < schema.sql
    duckdb --init init.sql todd.db < queries.sql

### Analysis

Any query can be prefixed with `EXPLAIN ANALYZE` to get more detailed info regarding query execution.

Another useful debug command details the Parquet configuration:

    FROM parquet_file_metadata('s3://todd/dictionary.parquet');


