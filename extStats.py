import duckdb

#Creates an external copy of the stats table from CIFF db file in parquet for easy loading and
#potantially efficient usage.


# Create/open DuckDB database.
con = duckdb.connect("./index.db")

# Copy the dict table from the database to an external parquet file
con.execute("""COPY ows.stats TO 'docStats.parquet' (FORMAT PARQUET)""")

# cleanup
con.close()