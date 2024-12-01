CREATE OR REPLACE MACRO tokenize(s) AS (
            string_split_regex(regexp_replace(lower(strip_accents(CAST(s AS VARCHAR))), '[0-9!@#$%^&*()_+={}\\[\\]:;<>,.?~\\\\/\\|''''"`-]+', ' ', 'g'), '\s+'));

create or replace temporary table qs as 
  select tokenize(QueryText) as qt from read_csv_auto('sample_queries10.csv');

create or replace temporary table q as
  select rowid as qno, unnest(qt) as token from qs;

create or replace temporary table query as
  select token as qterm from q where qno = 0;

