-- BM25 ranking
--
-- Postpone document scoring (group by docid) until after unioning posting sublists
--
WITH queryterms AS (
  SELECT termid, df FROM dict WHERE 
    (term = 'radboud' AND crange = ows.cr('radboud'))
  UNION
  SELECT termid, df FROM dict WHERE 
    (term = 'university' AND crange = ows.cr('university'))
),
termscores AS (
    SELECT 
      p.docid,
      LOG((SELECT num_docs FROM stats) / (1 + q.df)) * 
          (p.tf * (1.5 + 1)) / (p.tf + 1.5 * (1 - 0.75 + 0.75 * (d.len / (SELECT avgdl FROM stats)))
      ) AS bm25termscore
    FROM postings p, queryterms q, docs d
    WHERE (p.termid = 6609552) AND (p.thash = ows.p(6609552)) AND p.termid = q.termid AND (p.docid = d.docid)
  UNION
    SELECT 
      p.docid,
      LOG((SELECT num_docs FROM stats) / (1 + q.df)) * 
  	(p.tf * (1.5 + 1)) / (p.tf + 1.5 * (1 - 0.75 + 0.75 * (d.len / (SELECT avgdl FROM stats)))
      ) AS bm25termscore
    FROM postings p, queryterms q, docs d
    WHERE (p.termid = 7928317 AND p.thash = ows.p(7928317) AND p.termid = q.termid AND p.docid = d.docid)
),
docscores AS (
  SELECT docid, sum(bm25termscore) AS bm25score
  FROM termscores
  GROUP BY docid
)
SELECT ds.docid, ds.bm25score, di.name
FROM docscores ds
JOIN docs di ON ds.docid = di.docid
ORDER BY ds.bm25score DESC
LIMIT 20;

--  JOIN queryterms q ON p.termid = q.termid AND p.thash = q.termid % 100 

