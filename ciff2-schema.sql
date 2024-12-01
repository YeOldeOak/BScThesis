--
-- An alternative for CIFF using Parquet files
--
-- Research Question:
-- Can we avoid downloading large index files?
--
-- Parquet files can be queried on remote S3 storage 
-- With the right preparation, push-down filters may make query processing affordable
--

-- The CIFF loader creates its tables in schema ows
USE ows;

-- Store the views on S3 parquet files in its own schema
CREATE SCHEMA IF NOT EXISTS rciff2;

-- Statistics
COPY (FROM stats) TO 's3://ows/ciff2/stats.parquet' (FORMAT PARQUET, OVERWRITE_OR_IGNORE);
CREATE OR REPLACE VIEW rciff2.stats as FROM read_parquet('s3://ows/ciff2/stats.parquet');

-- Create ranges of strings based on their first character:
CREATE OR REPLACE MACRO cr(term) AS ascii(term[1]) // 10;

-- Create the Dictionary Parquet file using macro cr for partitioning:
COPY (SELECT term, termid, df, cr(term) AS crange FROM dict ORDER BY crange, termid) 
     TO 's3://ows/ciff2/dict.parquet' (FORMAT PARQUET, OVERWRITE_OR_IGNORE);
CREATE OR REPLACE VIEW rciff2.dict AS FROM read_parquet('s3://ows/ciff2/dict.parquet');

-- The Docs Parquet file
-- Relatively small at about 115MB, it does not benefit much from Hive partitioning
COPY (FROM docs) TO 's3://ows/ciff2/docs.parquet' (FORMAT PARQUET, OVERWRITE_OR_IGNORE);
CREATE OR REPLACE VIEW rciff2.docs AS FROM read_parquet('s3://ows/ciff2/docs.parquet');

-- Postings
--
-- The Postings table should be Hive Partitioned by a hash derived from termid,
-- in order for query processing to ignore as many rows as possible.
--
-- Repartitioning into zone maps ideally would follow the original order of the postings table,
-- i.e., the computed partition number of a given termid should not shuffle their order
--
-- The number of bits to shift the termids in the postings for a good partitioning
-- can be determined with the following SQL query:
--   WITH 
--     ptwos AS (SELECT m, 1<<m as ptwo FROM (SELECT UNNEST(RANGE(0,20)) as m)),
--     N     AS (SELECT count(*) FROM ows.postings),
--     tMax  AS (SELECT max(termid) FROM ows.postings),
--     G     AS (SELECT (16 * (FROM N)) // (100*1024*1024))
--   SELECT min(m) FROM ptwos WHERE ptwo > (FROM tMax) // (FROM G);
--
-- As a rule of thumb, Hive partitions should be about 100MB in size,
-- and, every posting takes 16 bytes (1 BIGINT for termid and 2 INTEGERs for docid and tf).

-- Create partitions based on a hash by termid
CREATE OR REPLACE MACRO p(termid) AS (termid >> 17)::TINYINT;

-- Postings Parquet file, hive paritioned on termid hash
COPY (
	SELECT p(termid) AS thash, termid, docid, tf FROM owsdd.postings
) TO 's3://ows/ciff2/postings.parquet' (FORMAT PARQUET, PARTITION_BY (thash), OVERWRITE_OR_IGNORE);
CREATE OR REPLACE VIEW rciff2.postings AS FROM read_parquet('s3://ows/ciff2/postings.parquet/*/*.parquet', 
	hive_partitioning=true, hive_types={'thash': TINYINT});

-- Physical details from the internal representation:
-- FROM parquet_file_metadata('s3://ows/ciff2/postings.parquet/*/*.parquet');

