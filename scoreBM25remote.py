import duckdb
import pandas as pd



con = duckdb.connect()

with open('s3-init-priv.sql', 'r') as s3_conn:
    s3_privs = s3_conn.read()
    con.execute(s3_privs)

# Create necessary views from the parquet files
con.execute("CREATE VIEW postings AS SELECT * FROM read_parquet('s3://todd/postings/*/*');")
con.execute("CREATE VIEW dict AS SELECT * FROM read_parquet('s3://todd/dictionary.parquet');")
con.execute("CREATE VIEW docs AS SELECT * FROM read_parquet('s3://todd/documentData.parquet');")
con.execute("CREATE VIEW stats AS SELECT * FROM read_parquet('s3://todd/docStats.parquet');")
print("loaded")

# random states for 10 queries was 37, for 100 queries it was 23
queryamount = 10
data = pd.read_csv(f'sample_queries{queryamount}.csv', header=None)
queries= data.iloc[1:, 0].apply(lambda query: tuple(query.split())).tolist()

topResults = []

# BM25 query, broken down: 
# first query section finds termids of the inputted query
# second query calculates bm25 scoring of each document based on posting data and term appearance
    # bm25 algorithm is normalized to the length of documents, in the 3rd line of the SUM section
for query in queries:
    scoreBM25 = f"""WITH queryterms AS (
            SELECT termid, df
            FROM dict
            WHERE term IN {query}
        ),
        docscores AS (
            SELECT 
                p.docid,
                SUM(
                    LOG((SELECT num_docs FROM stats) / (1 + q.df)) * 
                    (p.tf * (1.5 + 1)) /
                    (p.tf + 1.5 * (1 - 0.75 + 0.75 * (d.len / (SELECT avgdl FROM stats))))
                ) AS bm25score
            FROM 
                postings p
            JOIN queryterms q ON p.termid = q.termid
            JOIN docs d ON p.docid = d.docid
            GROUP BY p.docid
        )
        SELECT ds.docid, ds.bm25score, di.name
        FROM docscores ds
        JOIN docs di ON ds.docid = di.docid
        ORDER BY ds.bm25score DESC;"""
    print("loop done")    
    temp_result = con.execute(scoreBM25).fetchall()
    topResults.append(temp_result[:1])


print(topResults)



con.close()