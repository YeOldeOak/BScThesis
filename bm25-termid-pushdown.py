import duckdb
import time
import pandas as pd




# Connect to the DuckDB instance
con = duckdb.connect()

# Use secret
with open('s3-init-priv.sql', 'r') as s3_conn:
    s3_privs = s3_conn.read()
    con.execute(s3_privs)

# Load/create necessary views from remote parquet files
con.execute("CREATE OR REPLACE MACRO cr(term) AS ascii(term[1]) // 10;")
con.execute("CREATE VIEW postings AS SELECT * FROM read_parquet('s3://todd/postings/*/*');")
con.execute("CREATE VIEW dict AS SELECT * FROM read_parquet('s3://todd/pdict.parquet');")
con.execute("CREATE VIEW docs AS SELECT * FROM read_parquet('s3://todd/documentData.parquet');")
con.execute("CREATE VIEW stats AS SELECT * FROM read_parquet('s3://todd/docStats.parquet');")
print("loaded")

# Load random sample of ORCAS queries to a parsable format
# random states for 10 queries was 37, for 100 queries it was 23, for 500 queries it was 66, for 50 it was 17
queryamount = 50
data = pd.read_csv(f'sample_queries{queryamount}.csv', header=None)
queries = data.iloc[1:, 0].apply(lambda query: [term.replace("'", "") for term in query.split()]).tolist()


# Main function for building CTEs and running BM25 queries
def run_queries(queries):
    skipped_queries = 0
    
    for query in queries:

        # Extract termids of query terms in current query into a Python list
        termids_result = con.execute(f"""
        SELECT termid FROM dict
        WHERE term IN ({", ".join(f"'{term}'" for term in query)})
        AND crange IN ({", ".join(f"cr('{term}')" for term in query)});
        """).fetchall()

        # Query terms without an id will return nothing in the lines above
        # In the case an entire query returns nothing (eg. single term that doesn't have id), that is dealt with here
        if not termids_result:
            print(f"No terms found in dictionary for query: {query}")
            skipped_queries += 1
            continue

        # Isolate the termids from the result
        termids = [row[0] for row in termids_result]

        # Construct a CTE to filter the postings corresponding to the query terms
        postings_cte = " UNION ".join([
            f"""
            SELECT termid, docid, df, tf
            FROM postings
            JOIN dict USING (termid)
            WHERE termid = {termid} AND thash = MOD(hash({termid}), 40)
            """ 
            for termid in termids
        ])

        # Combine everything into the final SQL query
        scoreBM25 = f"""
        WITH filtered_postings AS (
        {postings_cte}
        ),
        termscores AS (
        SELECT
            p.docid,
            LOG((SELECT num_docs FROM stats) / (1 + p.df)) *
            (p.tf * (1.5 + 1)) /
            (p.tf + 1.5 * (1 - 0.75 + 0.75 * (d.len / (SELECT avgdl FROM stats)))) AS bm25termscore
        FROM filtered_postings p
        JOIN docs d USING (docid)
        ),
        docscores AS (
        SELECT docid, SUM(bm25termscore) AS bm25score
        FROM termscores
        GROUP BY docid
        )
        SELECT ds.docid, ds.bm25score
        FROM docscores ds
        ORDER BY ds.bm25score DESC;
        """    

        # print(scoreBM25)
        # This line is to be modified if actual values are to be returned. For timing purposes
        # Explain analyze cannot be used, as it doesn't take into account, at the minimum, lookups for termids
        main_query_result = con.execute(scoreBM25).fetchall()

    return skipped_queries
      



# Measure total time
start_time = time.time()      

# Run all queries
nr_skipped = run_queries(queries)

# Formatted for minutes only
end_time = time.time()
print(f"Total time: {(end_time - start_time)/60:.4f} minutes")
print(f"Average time: {(end_time - start_time)/(queryamount - nr_skipped):.4f} seconds")

# Close connection after use
con.close()
