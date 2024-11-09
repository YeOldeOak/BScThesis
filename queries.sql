CREATE OR REPLACE VIEW dict AS FROM read_parquet('s3://todd/dictionary.parquet');

-- Normal query term selection
SELECT termid, df FROM dict WHERE term IN ( 'radboud', 'university');

-- My version of the dictionary Parquet file:
COPY (SELECT term, termid, df, cr(term) AS crange FROM dict ORDER BY crange) 
     TO 's3://test/pdict.parquet' (FORMAT PARQUET, OVERWRITE_OR_IGNORE);
CREATE OR REPLACE VIEW dict AS FROM read_parquet('s3://test/pdict.parquet');

-- Using extra constraints on first character
SELECT termid, df FROM dict WHERE 
   (term = 'radboud') AND (crange=cr('radboud')) 
OR (term = 'university') AND (crange=cr('university'));

-- UNION variant
SELECT termid, df FROM dict WHERE (term = 'radboud')
UNION
SELECT termid, df FROM dict WHERE (term = 'university');

-- UNION variant with extra constraint
SELECT termid, df FROM dict WHERE 
  (term = 'radboud' AND crange = cr('radboud'))
UNION
SELECT termid, df FROM dict WHERE 
  (term = 'university' AND crange = cr('university'));

-- BM25 ranking
WITH queryterms AS (
  SELECT termid, df FROM dict WHERE 
    (term = 'radboud' AND crange = cr('radboud'))
  UNION
  SELECT termid, df FROM dict WHERE 
    (term = 'university' AND crange = cr('university'))
),
docscores AS (
  SELECT 
    p.docid,
    SUM(
	LOG((SELECT num_docs FROM stats) / (1 + q.df)) * (p.tf * (1.5 + 1)) /
	  (p.tf + 1.5 * (1 - 0.75 + 0.75 * (d.len / (SELECT avgdl FROM stats))))) AS bm25score
  FROM postings p, queryterms q, docs d
  WHERE (p.termid = 11313573 AND p.thash = 29 AND p.termid = q.termid AND p.docid = d.docid)
  GROUP BY p.docid
UNION
  SELECT 
    p.docid,
    SUM(
	LOG((SELECT num_docs FROM stats) / (1 + q.df)) * (p.tf * (1.5 + 1)) /
	  (p.tf + 1.5 * (1 - 0.75 + 0.75 * (d.len / (SELECT avgdl FROM stats))))) AS bm25score
  FROM postings p, queryterms q, docs d
  WHERE (p.termid =  9430643 AND p.thash = 25 AND p.termid = q.termid AND p.docid = d.docid)
  GROUP BY p.docid
)
SELECT ds.docid, ds.bm25score, di.name
FROM docscores ds
JOIN docs di ON ds.docid = di.docid
ORDER BY ds.bm25score DESC;

--  JOIN queryterms q ON p.termid = q.termid AND p.thash = q.termid % 100 


