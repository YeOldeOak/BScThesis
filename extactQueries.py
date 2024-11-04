import pandas as pd

queryamount = 10;


# Load ORCAS data into a Pandas DataFrame (assuming 'orcas.tsv' is the extracted file)
orcas_df = pd.read_csv('./orcas-doctrain-queries.tsv/orcas-doctrain-queries.tsv', sep='\t', header=None, names=['QueryID', 'QueryText', 'DocumentID'])

# Randomly sample `amount` of unique queries
sample_queries = orcas_df['QueryText'].drop_duplicates().sample(n=queryamount, random_state=37)

# Save the sampled queries to .csv
sample_queries.to_csv(f'sample_queries{queryamount}.csv', index=False)

# # This is how you can read the queries in a tuple formatting for use in querying
# data = pd.read_csv(f'sample_queries{queryamount}.csv', header=None)
# queries = data.iloc[1:, 0].apply(lambda query: tuple(query.split())).tolist()
