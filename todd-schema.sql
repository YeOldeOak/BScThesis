-- Todd:
CREATE OR REPLACE VIEW stats AS FROM read_parquet('s3://todd/docStats.parquet');
CREATE OR REPLACE VIEW docs AS FROM read_parquet('s3://todd/documentData.parquet');
CREATE OR REPLACE VIEW dict AS FROM read_parquet('s3://todd/dictionary.parquet');
CREATE OR REPLACE VIEW postings AS FROM read_parquet('s3://todd/postings40/*/*.parquet');
