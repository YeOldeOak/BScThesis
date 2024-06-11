import duckdb


# Functions

def fetch_num_rows(connection, source_table):
    rows_query = f"""
                 SELECT COUNT(*)
                 FROM {source_table}
                 """
    return connection.execute(rows_query).fetchone()[0]



# Function to determine if a column for hashing already exists or needs to be created.
def column_creation(connection, table_name, column_name):
    columnexists_query = f"""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = '{table_name}' 
        AND column_name = '{column_name}'
        """
    
    result = connection.execute(columnexists_query).fetchall()
    if len(result) == 0:
        connection.execute(f"""
                ALTER TABLE {table_name}
                ADD COLUMN {column_name}
                """)



# Function to hash in chunks.
# Uses the update query, broken up into defined sections to avoid OOM issues.
def hash_in_chunks(connection, table_name, id_column, chunk_size):
    # Indicate chunk start for each chunk:
    offset = 0
    # Get total number of rows in table:
    total_rows = fetch_num_rows(connection, table_name)


    # Compute hashes in chunks:
    while offset < total_rows:
        # Update query for computing chunks:
        update_query = f"""
        UPDATE {table_name}
        SET thash = (hash(termid) % 100)
        WHERE {id_column} >= {offset} AND {id_column} <= {offset + chunk_size}
        """


        # Update chunk:
        connection.execute(update_query)
        # Increase offset to next chunk:
        offset += chunk_size





# Create/open DuckDB database.
con = duckdb.connect("./ciff.db")

# Add new column to postings table if it doesn't already exist, to store hash values.
# Type is INT, as regardless of how big of a value hash gives us, we still mod.
column_creation(con, 'postings', 'thash')

# Perform a computation to retreive a value representing the partition the term will belong to.
# To increase even distribution, perhaps consider casting termid and tf to string and concatinating them
# before hashing, this will require more computation however and thus more overhead.

print("hash start")
#con.execute("""UPDATE postings SET thash = termid % 100""")
hash_chunk_size = 50000
hash_in_chunks(con, 'postings', 'termid', hash_chunk_size)
print("hash finish")

# Partition the data based on previously calculated values in chunks.
print("partition start")
con.execute("""COPY postings TO 'postings' (FORMAT PARQUET, PARTITION_BY thash)""")
print("partition finish")

# Cleanup.
con.close()