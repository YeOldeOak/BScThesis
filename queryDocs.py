import duckdb


# Functions



def fetch_num_rows(connection, source_table):
    rows_query = f"""
                 SELECT COUNT(*)
                 FROM {source_table}
                 """
    return connection.execute(rows_query).fetchone()[0]



# Find if query term exists in the dictionary. 
# This is not currently possible without loading the ciff.db as that's where the dictionary is.
def termExists(connection, query_term, dictionary_table, term_column):
    # Query to see if term exists in specificed table in specificed column:
    query = f"SELECT EXISTS (SELECT 1 FROM {dictionary_table} WHERE {term_column} = '{query_term}')"

    # If it exists, query will return True, otherwise it will return False.
    result = connection.execute(query).fetchone()[0]
    return result



# Returns the termId of query term if it exists, if it doesn't, then -1 is returned.
# This function also requires the dictionary table.
def getTermId(connection, query_term, dictionary_table, term_column, id_column):
    if(termExists(connection, query_term, dictionary_table, term_column)):
        # Query to get termid value from given term:
        query = f"SELECT {id_column} FROM {dictionary_table} WHERE {term_column} = '{query_term}'"

        # Return the id.
        result = connection.execute(query).fetchone()[0]
        return result
    else:
        # If the term doesn't exists, we return -1
        return -1;



def getDocsForTerm(connection, query_term, dictionary_table, dict_term_column, dict_termid_columnn):
    # We get the termid from given term:
    query_term_id = getTermId(connection, query_term, dictionary_table, dict_term_column, dict_termid_columnn)
    # We find the bucket the term is located in by hashing and modding:
    bucket = connection.execute(f"select (hash({query_term_id}) % 100)").fetchone()[0]
    # Directory to the bucket's parquet file:
    bucket_dir = f"./postings/thash={bucket}/*.parquet"
    # Query to find all docid's from a specific parquet bucket where the termid is the one we are looking for.
    query = f"SELECT docid FROM read_parquet('{bucket_dir}') WHERE termid = {query_term_id}"

    # execute the query
    result_tuples = duckdb.sql(query).fetchall()
    # We need only the first value of each tuple that is returned via fetchall, thus we only select those and return as a list.
    docList = [row[0] for row in result_tuples]
    return docList





    






# Create/open DuckDB database.
con1 = duckdb.connect("./ciff.db")

x = getDocsForTerm(con1, "runescape", "dict", "term", "termid")
print(x)

# Cleanup.
con1.close()