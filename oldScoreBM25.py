import duckdb
import queryTerms as qT


con = duckdb.connect()

# Create necessary views from the parquet files
con.execute("CREATE VIEW postings AS SELECT * FROM read_parquet('./postings/*/*')")
con.execute("CREATE VIEW dict AS SELECT * FROM read_parquet('./dictionary.parquet')")
con.execute("CREATE VIEW docs AS SELECT * FROM read_parquet('./documentData.parquet')")
con.execute("CREATE VIEW stats AS SELECT * FROM read_parquet('./docStats.parquet')")

testQuery = ('playing', 'runescape', 'on', 'weekends')

# BM25 query, broken down: 
# first query section finds termids of the inputted query
# second query calculates bm25 scoring of each document based on posting data and term appearance
    # bm25 algorithm is normalized to the length of documents, in the 3rd line of the SUM section
scoreBM25 = f"""WITH queryterms AS (
    SELECT termid, df
    FROM dict
    WHERE term IN {testQuery}
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


result = con.execute(scoreBM25).fetchall()
print(result[:15])



con.close()
