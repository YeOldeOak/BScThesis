import duckdb
import time
import re

# Connect to the DuckDB instance
con = duckdb.connect()

# Use secret
with open('s3-init-priv.sql', 'r') as s3_conn:
    s3_privs = s3_conn.read()
    con.execute(s3_privs)

# Create necessary views from the parquet files
con.execute("CREATE OR REPLACE MACRO cr(term) AS ascii(term[1]) // 10;")
con.execute("CREATE VIEW postings AS SELECT * FROM read_parquet('s3://todd/postings40/*/*');")
con.execute("CREATE VIEW dict AS SELECT * FROM read_parquet('s3://todd/pdict.parquet');")
con.execute("CREATE VIEW docs AS SELECT * FROM read_parquet('s3://todd/documentData.parquet');")
con.execute("CREATE VIEW stats AS SELECT * FROM read_parquet('s3://todd/docStats.parquet');")
print("loaded")


# Sample set of queries, each as a list of terms
queries = [
    ['radboud', 'university'],
    ['research', 'data', 'science'],
    ['open', 'access'],
    ['artificial', 'intelligence', 'ethics'],
    ['machine', 'learning', 'neural', 'network']]

def run_queries(queries):
    for query in queries:
         # Measure total time
        start_time = time.time()

        # Build the queryterms CTE
        queryterms_cte = f"""
        WITH queryterms AS (
            SELECT term, termid, df, crange
            FROM dict
            WHERE term IN ({", ".join(f"'{term}'" for term in query)})
        )
        """

        # Extract termids into a Python list
        termids_result = con.execute(f"""
        SELECT termid FROM dict
        WHERE term IN ({", ".join(f"'{term}'" for term in query)});
        """).fetchall()
        termids = [row[0] for row in termids_result]

        # Construct the termscores CTE
        termscores_cte = " UNION ".join([
            f"""SELECT 
                    p.docid,
                    LOG((SELECT num_docs FROM stats) / (1 + q.df)) * 
                        (p.tf * (1.5 + 1)) / 
                        (p.tf + 1.5 * (1 - 0.75 + 0.75 * (d.len / (SELECT avgdl FROM stats)))
                    ) AS bm25termscore
                FROM postings p, queryterms q, docs d
                WHERE (p.termid = {termid}
                    AND p.thash = MOD(hash('{query[termids.index(termid)]}'), 40) 
                    AND p.termid = q.termid 
                    AND p.docid = d.docid)"""    
            for termid in termids
        ])

        # Combine everything into the final SQL query
        scoreBM25 = f"""
        {queryterms_cte},
        termscores AS (
            {termscores_cte}
        ),
        docscores AS (
            SELECT docid, SUM(bm25termscore) AS bm25score
            FROM termscores
            GROUP BY docid
        )
        SELECT ds.docid, ds.bm25score, di.name
        FROM docscores ds
        JOIN docs di ON ds.docid = di.docid
        ORDER BY ds.bm25score DESC;
        """

        

        result = con.execute(f"EXPLAIN ANALYZE {scoreBM25}").fetchall()[0][1]

         # Calculate the total execution time (this will include termid lookup and BM25 query)
        end_time = time.time()
        print(f"Total time: {end_time - start_time:.4f} seconds")


      

# Run all queries
run_queries(queries)

# Close connection after use
con.close()
