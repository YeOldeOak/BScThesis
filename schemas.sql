-- Macro to create ranges of strings based on first character of string:
CREATE OR REPLACE MACRO cr(term) AS ascii(term[1]) // 10;

-- Todd:
CREATE OR REPLACE VIEW stats AS FROM read_parquet('s3://todd/docStats.parquet');
CREATE OR REPLACE VIEW docs AS FROM read_parquet('s3://todd/documentData.parquet');
CREATE OR REPLACE VIEW dict AS FROM read_parquet('s3://todd/dictionary.parquet');
CREATE OR REPLACE VIEW postings AS FROM read_parquet('s3://todd/postings40/*/*.parquet');

-- My version of the dictionary Parquet file:
COPY (SELECT term, termid, df, cr(term) AS crange FROM dict ORDER BY crange) 
     TO 's3://test/pdict.parquet' (FORMAT PARQUET, OVERWRITE_OR_IGNORE);
CREATE OR REPLACE VIEW dict AS FROM read_parquet('s3://test/pdict.parquet');

