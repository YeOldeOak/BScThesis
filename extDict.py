import duckdb

#Creates an external copy of the dictionary table from CIFF db file in parquet for easy loading and
#potantially efficient usage.


# Create/open DuckDB database.
con = duckdb.connect("./index.db")

# Copy the dict table from the database to an external parquet file
con.execute("""COPY owsdd.Dict TO 'dictionary.parquet' (FORMAT PARQUET)""")

# cleanup
con.close()