import pandas as pd

def modify_mpa_data_types(df):
	df["date"] = pd.to_datetime(df["date"])
	for heading in ['file_no','api_no','mcf_gas','days_produced','mcf_sold','mcf_flared']:
		df[heading] = pd.to_numeric(df[heading])
	return df

def modify_wi_data_types(df):
    df["SpudDate"] = pd.to_datetime(df["SpudDate"])
    return df


def print_groups_head(grouped):
	i = 0
	for key, item in grouped:
		print(grouped.get_group(key), "\n\n")
		i += 1
		if (i > 4):
			return


def calc_everything_else(df):
	grouped = df.groupby(['file_no', 'api_no', 'pool'])
	summed = grouped.agg(['sum'])
	return summed


# Calculates the amount of flaring that occurred starting one year after SpudDate.
# First filters the datapoints that fall within the grace period, then sums the rest.
# https://stackoverflow.com/questions/45597499/filter-pandas-dataframe-by-comparing-columns-in-a-row
def calc_flaring(df):
	filtered_df = df[df['date'] > df['grace_period_end']]    ############## <= ????
	grouped = filtered_df.groupby(['file_no', 'api_no', 'pool', 'grace_period_end'])
	summed = grouped.agg(['sum'])

	print("groups", len(grouped))
	print("summed", len(summed))

	# Just renaming this column
	summed.columns = ['mcf_flared_after_grace_period']

	print("summed", len(summed))
	print("FILTERED", summed.head())
	return summed


def calc_dates(df):
	earliest_records_df = df.loc[df.groupby(['file_no', 'api_no', 'pool']).date.idxmin()]
	
	#new_date = row['SpudDate'] + pd.tseries.offsets.DateOffset(years=1)


# Load .csv data, set indices on the dataframes.
mpa_df = pd.read_csv("test-mpa.csv")
mpa_df = modify_mpa_data_types(mpa_df)

wi_df = pd.read_csv("test-wi.csv")
wi_df = modify_wi_data_types(wi_df)

# This is going to save us some confusion.
wi_df.rename(columns={"FileNo": "file_no"}, inplace=True)

# Remove some columns we don't care about today.
mpa_df.drop('bbls_oil', axis=1, inplace=True)
mpa_df.drop('bbls_water', axis=1, inplace=True)
mpa_df.drop('oil_sold', axis=1, inplace=True)

# Extract just the column we need to merge with the monthly prod data
print("Determining (conservative) date of first extraction and end of 1yr flaring grace period.")
print("NOTE: Even if we conservatively calculate a production start date to the day,\nwe do not have that granularity on the 'mcf_flared' data,\n so we give them the rest of the partial month.\n To be conservative, the well operators could get up to 30 days extra\ngrace period if they produced only 1 day in a month.")
print("NOTE 2: If we have not scraped data back to the first production date of the well, I am assuming they have the first 12 months of records as a grace period. We can scrape back further to minimize this issue. (ex, well started production in Feb 2001, but we only have date to 2005, we presume they started production in the first month of data we have available.)")

# Create new column from spuddate by adding 1 years
print("Calculating 1yr grace period end date")
dates_added_df = calc_dates(mpa_df)

# Merge SpudDates and offset dates into mpa_df
print("Merging tables")
df = pd.merge(mpa_df, dates_added_df, left_on="file_no", right_on="file_no", how='left')


# Prepare a separate dataframe for running different aggregations on.
calc_flaring_df = df[['file_no', 'api_no', 'pool','date', 'grace_period_end', 'mcf_flared']].copy()

df.set_index(['file_no', 'api_no', 'pool', 'date'], inplace=True)
calc_flaring_df.set_index(['file_no', 'api_no', 'pool'], inplace=True)


# Create a new table with just file_no and illegal flare amount columns
print("Aggregating flare volume after grace period.")
flared_df = calc_flaring(calc_flaring_df)

print("FILTERED", flared_df.head())

# Aggregate sums of all other records
print("Aggregating other well records.")
aggregated_df = calc_everything_else(df)

# Removed (sum) from the column labels
aggregated_df.columns = map(lambda column: column[0], aggregated_df.columns.values)

# Set indices to match before merging on them.
flared_df.reset_index(inplace=True)
flared_df.set_index(['file_no'], inplace=True)
aggregated_df.reset_index(inplace=True)
aggregated_df.set_index(['file_no'], inplace=True)

# Drop some columns before merge to avoid duplicate data.
flared_df.drop('pool', axis=1, inplace=True)
flared_df.drop('api_no', axis=1, inplace=True)

print("AGGREGATED_DF", aggregated_df)
print("FLARED_DF", flared_df)
final_df = pd.merge(flared_df,aggregated_df, how='left', left_index=True, right_index=True)

print("RESULT", final_df.head(5))

# Sort the results so we can find the worst offenders!
print("Ranking by most mcf_flared.")
sorted_flared_df = final_df.sort_values(by=['mcf_flared_after_grace_period'], ascending=False)
print(sorted_flared_df.head(30))
#once top values are determined, print those indices before sorting to verify this function works as i think it does.


print("Done!")