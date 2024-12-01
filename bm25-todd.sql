-- Todd's BM25 ranking
--
-- Without using the term hash
--

USE rciff2;

WITH queryterms AS (
  SELECT termid, df FROM dict WHERE 
    (term = 'radboud' AND crange = ows.cr('radboud'))
  UNION
  SELECT termid, df FROM dict WHERE 
    (term = 'university' AND crange = ows.cr('university'))
),
docscores AS (
  SELECT 
    p.docid,
    SUM(
	LOG((SELECT num_docs FROM stats) / (1 + q.df)) * (p.tf * (1.5 + 1)) /
	  (p.tf + 1.5 * (1 - 0.75 + 0.75 * (d.len / (SELECT avgdl FROM stats))))) AS bm25score
  FROM postings p
  JOIN queryterms q ON p.termid = q.termid 
  JOIN docs d ON p.docid = d.docid
  GROUP BY p.docid
)
SELECT ds.docid, ds.bm25score, di.name
FROM docscores ds
JOIN docs di ON ds.docid = di.docid
ORDER BY ds.bm25score DESC
LIMIT 20;



