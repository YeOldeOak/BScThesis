import duckdb
import pandas as pd
import time

# Function to measure the time for a simple SELECT query
def execute_simple_query(con, view_name, limit=1):
    query = f"SELECT * FROM {view_name} LIMIT {limit};"
    start_time = time.time()
    con.execute(query)
    end_time = time.time()
    return end_time - start_time

# Function to measure the count of rows in a view
def count_rows(con, view_name):
    query = f"SELECT COUNT(*) FROM {view_name};"
    start_time = time.time()
    row_count = con.execute(query).fetchone()[0]
    end_time = time.time()
    return row_count, end_time - start_time

# Function to measure data retrieval time from MinIO
def measure_data_retrieval_time(con):
    start_time = time.time()
    con.execute("SELECT * FROM postings LIMIT 1;")  # Simple query to read a sample record
    end_time = time.time()
    return end_time - start_time

# Function to fetch sample data from the docs view
def fetch_sample_docs(con, limit=5):
    query = f"SELECT * FROM docs LIMIT {limit};"
    start_time = time.time()
    sample_data = con.execute(query).fetchall()
    end_time = time.time()
    return sample_data, end_time - start_time

def main():
    # Connect to DuckDB
    con = duckdb.connect()

    # Load S3 connection privileges from SQL file
    with open('s3-init-priv.sql', 'r') as s3_conn:
        s3_privs = s3_conn.read()
        con.execute(s3_privs)

    # Create necessary views from the parquet files
    con.execute("CREATE VIEW postings AS SELECT * FROM read_parquet('s3://todd/postings40/*/*');")
    con.execute("CREATE VIEW dict AS SELECT * FROM read_parquet('s3://todd/dictionary.parquet');")
    con.execute("CREATE VIEW docs AS SELECT * FROM read_parquet('s3://todd/documentData.parquet');")
    con.execute("CREATE VIEW stats AS SELECT * FROM read_parquet('s3://todd/docStats.parquet');")
    print("Loaded views.")

    # Measure execution time for simple SELECT queries on each view
    views = ['postings', 'dict', 'docs', 'stats']
    for view in views:
        execution_time = execute_simple_query(con, view)
        print(f"Execution time for fetching from {view}: {execution_time:.4f} seconds")

    # Measure the number of rows and time taken for the docs view
    docs_count, docs_count_time = count_rows(con, 'docs')
    print(f"Number of rows in docs: {docs_count}, Time taken: {docs_count_time:.4f} seconds")

    # Measure data retrieval time from MinIO
    retrieval_time = measure_data_retrieval_time(con)
    print(f"Data retrieval time from MinIO for postings: {retrieval_time:.4f} seconds")

    # Fetch a sample of documents from the docs view
    sample_docs, sample_time = fetch_sample_docs(con)
    print(f"Sample docs fetched: {sample_docs}, Time taken: {sample_time:.4f} seconds")

    con.close()

if __name__ == "__main__":
    main()
