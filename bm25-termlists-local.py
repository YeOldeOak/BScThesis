import duckdb
import pandas as pd
import time




# Load/create necessary views from local duckdb database file
con = duckdb.connect('index.db')
con.execute("CREATE OR REPLACE MACRO cr(term) AS ascii(term[1]) // 10;")
con.execute("CREATE OR REPLACE VIEW postings AS SELECT * FROM index.owsdd.postings;")
con.execute("CREATE OR REPLACE VIEW dict AS SELECT * FROM index.owsdd.dict;")
con.execute("CREATE OR REPLACE VIEW docs AS SELECT * FROM index.owsdd.docs;")
con.execute("CREATE OR REPLACE VIEW stats AS SELECT * FROM index.ows.stats;")

# Random states for 10 queries was 37, for 100 queries it was 23, for 500 queries it was 66, for 50 it was 17
# Load random sample of ORCAS queries to a parsable format
queryamount = 50
data = pd.read_csv(f'sample_queries{queryamount}.csv', header=None)
queries = data.iloc[1:, 0].apply(lambda query: [term.replace("'", "''") for term in query.split()]).tolist()



# Main function for building CTEs and running BM25 queries
def run_queries(queries):
    for query in queries:

        # Build the queryterms CTE
        queryterms_cte = f"""
        WITH queryterms AS (
            SELECT term, termid, df
            FROM dict
            WHERE term IN ({", ".join(f"'{term}'" for term in query)})
        )
        """

        # Extract termids of query terms in current query into a Python list
        termids_result = con.execute(f"""
        SELECT termid FROM dict
        WHERE term IN ({", ".join(f"'{term}'" for term in query)});
        """).fetchall()

        # Query terms without an id will return nothing in the lines above
        # In the case an entire query returns nothing (eg. single term that doesn't have id), that is dealt with here
        if not termids_result:
            print(f"No terms found in dictionary for query: {query}")
            continue

        # Isolate the termids from the result
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
                    AND p.thash = MOD(hash({termid}), 40) 
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

        # This line is to be modified if actual values are to be returned. For timing purposes
        # Explain analyze cannot be used, as it doesn't take into account, at the minimum, lookups for termids
        result = con.execute(scoreBM25).fetchall()



# Measure total time
start_time = time.time()     

# Run all queries
run_queries(queries)

end_time = time.time()
print(f"Total time: {end_time - start_time:.4f} seconds")

# Close connection after use
con.close()