-- Dealing with documents that occur multiple times in the CIFF file
-- A brief analysis of possible impact

-- Q: Can you give an example of documents captured multiple times on a day?
-- A: Here is a document crawled 3 times on Nov 5th, 2024:
create or replace view metadata as 
  from read_parquet( '/export/data2/arjen/OWS.EU/owi/public/main/1c245f32-9bf6-11ef-b3b5-0242c0a8c004/year=2024/month=11/day=5/language=eng/metadata_*.parquet' );
select id,record_id,title from metadata where id = '7fde268681b63259025d66cf4cc1398d0e64ae456ff0ec809cf2c6e795c18ea5';

-- Q: How many documents have duplicates in the index, really?
-- A: 505 (out of 1921321)
select count(*) as Ndup from (
  select docid from ows.docs group by docid having count(docid)>1 );

-- Q: Which documents are captured multiple times but with different lengths?
select * from ows.docs d, fixedciff.docs d2 where d.docid = d2.docid and d.len <> d2.len;

-- Should doc frequencies really be corrected?
-- A: Yes, quite a large number of terms in the dictionary needs corrections (13600)
select count(*) from fixedciff.dict d, ows.dict d0
where d.termid = d0.termid and d.df <> d0.df;

-- A: However, only few (36) of these corrections are larger than 2.
select count(*) from fixedciff.dict d, ows.dict d0 
where d.termid = d0.termid and d.df < d0.df - 2;

--    This does include common words like "germany", "dispute", and "neither",
--    but also informative, more rare words like "seybold" and "handelsregister".
select d.term, d.df from fixedciff.dict d, dict d0 
where d.termid = d0.termid and d.df < d0.df - 2;

