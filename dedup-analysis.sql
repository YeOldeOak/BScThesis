-- Dealing with documents that occur multiple times in the CIFF file
-- A brief analysis of possible impact

-- Can you give an example?
-- Here is a document crawled 3 times on Nov 5th, 2024:
select id,record_id,title from t where id = '7fde268681b63259025d66cf4cc1398d0e64ae456ff0ec809cf2c6e795c18ea5';

-- Q: How many documents have duplicates in the index, really?
-- A: 505 (out of 1921321)
select count(*) as Ndup from (
  select docid from docs group by docid having count(docid)>1 );

-- Should doc frequencies really be corrected?
-- A: Yes, quite a large number of terms in the dictionary needs corrections (13600)
select count(*) from dict_ud, dict 
where dict_ud.termid = dict.termid and dict_ud.df <> dict.df;

-- A: However, only few (36) of these corrections are larger than 2.
select count(*) from dict_ud, dict 
where dict_ud.termid = dict.termid and dict_ud.df < dict.df - 2;

--    This does include common words like "germany", "dispute", and "neither",
--    but also informative, more rare words like "seybold" and "handelsregister".
select dict.term, dict.df from dict_ud, dict 
where dict_ud.termid = dict.termid and dict_ud.df < dict.df - 2;
