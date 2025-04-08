-- Macro to create ranges of strings based on first character of string:
CREATE OR REPLACE MACRO ows.cr(term) AS ascii(term[1]) // 10;

-- My version of the dictionary Parquet file:
COPY (SELECT term, termid, df, cr(term) AS crange FROM dict ORDER BY crange) 
     TO 's3://test/pdict.parquet' (FORMAT PARQUET, OVERWRITE_OR_IGNORE);
CREATE OR REPLACE VIEW dict AS FROM read_parquet('s3://test/pdict.parquet');

