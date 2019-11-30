from bs4 import BeautifulSoup
from datetime import datetime
from helpers import to_sql_friendly, get_name, get_pass, get_start_year, get_start_month, build_scrape_dates, get_month, create_outdir
from multiprocessing import Pool
from os import path, mkdir, getcwd
from io import BytesIO
import time, sys, requests, zipfile, sqlite3
import pandas as pd


#prompt user for login credentials
def get_name_and_pw():
    print("You will need your login information for https://www.dmr.nd.gov/oilgas/")
    global username
    global password
    username = get_name()
    password = get_pass()


def requests_scrape(date):

	# Set up variables for query
	form_month = date[0]
	form_year = date[1]
	url = 'https://www.dmr.nd.gov/oilgas/feeservices/stateprod.asp'
	req_data = {'VTI-GROUP': '0', 'SELECTMONTH': str(form_month), 'SELECTYEAR': str(form_year), 'B1': 'Get State Volumes'}
	tries = 0

	date_text = "{}-{}".format(get_month(date[0]), date[1])
	print("Beginning scrape for month of {}".format(date_text))

	while True:
		try:
			r = requests.post(url, data=req_data, auth=(username, password))

			# Use Beautiful Soup to get the HTML table
			soup = BeautifulSoup(r.text, 'html.parser')
			table = soup.find(lambda tag: tag.name=='table' and tag.has_attr('id') and tag['id']=='largeTableOutput')

			if table:
				t_headers = list()
				for th in table.find_all("th"):
					# remove any newlines and extra spaces from left and right
					t_headers.append(th.text.replace('\n', ' ').strip())
				t_rows = table.find_all(lambda tag: tag.name=='tr')
			else:
				print("No data for month of {}".format(date_text))
				return

			# Parse the table rows
			df = parse_table(t_headers, t_rows, form_month, form_year)
			print("{} records found for {}".format(len(df), date_text))

			return df

		except requests.exceptions.Timeout:
			tries += 1
			# Try the scrape 5 times before failing
			if tries < 5:
				time.sleep(5)
				continue
		break

#def create_table(connection, table_name, column_names):
#	if not table_exists():
#		# create_table

		######
#	if table_name == monthly_prod_tn:
		# set unique contraints
#		None
		######

def parse_table(headers, rows, month, year):
	# Get rows as a list of lists
	data = [list(row.stripped_strings) for row in rows]

	# Load table to pandas dataframe
	df = pd.DataFrame.from_records(data[1:], columns=headers)	
	df = modify_data_types(df)

	# Name file (containing one month of data) and output a file.
	outname = '{}-{:02d}.csv'.format(year, month)
	fullname = path.join(outdir, "by_month", outname)

	# Save to CSV
	df.to_csv(fullname, index=False)
	return df

def modify_data_types(df):
	df["Date"] = pd.to_datetime(df["Date"], format="%m-%Y")
	for heading in ['File No', 'API No', 'Pool', 'Date', 'BBLS Oil', 'BBLS Water', 'MCF Gas', 'Days Produced', 'Oil Sold', 'MCF Sold', 'MCF Flared']:
		df[heading] = pd.to_int(df[heading])
	return df

if __name__ == "__main__":
	# Global variables
	outdir = None
	start_year = None
	today = datetime.now()
	well_index_url = 'https://www.dmr.nd.gov/oilgas/feeservices/flatfiles/Well_Index.zip'
	monthly_prod_tn ='MONTHLY_PRODUCTION'
	wi_tn = 'WELL_INDEX'

	if (path.exists("credentials.py")):
		from credentials import username, password
	else:
		username = None  
		password = None
		get_name_and_pw()

	start_year = get_start_year()
	instance_count = 6
	
	if username and password and start_year:
		outdir = create_outdir(today)

		print("Retrieving Well Index file.")
		resp = requests.get(well_index_url, auth=(username, password))
		print ("Unzipping Well Index file.")
		wi_bytes = BytesIO(resp.content)
		wi_zip = zipfile.ZipFile(wi_bytes)
		unzipped = []
		for name in wi_zip.namelist():
			unzipped.append(wi_zip.read(name))

		with open(path.join(outdir, "Well_Index.csv"), 'wb') as filehandle:
		    filehandle.write(unzipped[0])
		
		scrape_dates = build_scrape_dates(start_year)
		
		# Scrapes the monthly production data and outputs a .csv file for every month
		print("Scraping monthly production data with {} instances.".format(instance_count))
		p = Pool(instance_count)
		monthly_dfs = p.map(requests_scrape, scrape_dates)
		p.terminate()
		p.join()

		# Combines all data into one .csv
		print("Aggregating monthly production data.")
		amp_df = pd.concat(monthly_dfs, axis=0)
		amp_df.columns = to_sql_friendly(amp_df.columns)
		results_name = "Well Production Data for {}-{} to {}-{}".format(get_month(1), start_year, get_month(today.month), today.year)
		amp_df.to_csv(path.join(outdir, "Monthly Production Aggregated.csv"), index=False)

		# Builds a new sqlite3 table for monthly production data
		print("Building MONTHLY_PRODUCTION SQLite3 table.")
		sqlite_filename = path.join(outdir, "results.sqlite3")
		conn = sqlite3.connect(sqlite_filename)
		amp_df.to_sql(monthly_prod_tn, conn, if_exists='append', index=False)

		# Builds a new sqlite3 table from the well index file
		print("Building WELL_INDEX SQLite3 table.")
		wi_df = pd.read_csv(BytesIO(unzipped[0]))
		wi_df.columns = to_sql_friendly(wi_df.columns)
		wi_df.to_sql(wi_tn, conn, if_exists='append', index=False)
		conn.close()

		# Creates or updates a sqlite3 database in the program root that can continuously be added to. 
		print("Updating master SQLite3 DB.")
		sqlite_master_file = "master.sqlite3"
		mstr_conn = sqlite3.connect(sqlite_master_file)
		wi_df.columns = to_sql_friendly(wi_df.columns)
		wi_df.to_sql(wi_tn, mstr_conn, if_exists='append', index=False)
		amp_df.to_sql(monthly_prod_tn, mstr_conn, if_exists='append', index=False)
		# Add UNIQUE contraint across first four columns (File No, API No, Pool, and Date)
		# to ensure no duplicate entries are added on subsequent runs.



		#con = sqlite3.connect("master.sqlite3")

		# Test to see if constraints exist		
		#cur = con.cursor()
		#cur.execute("select sql from sqlite_master where type='table' and name='{}'".format(monthly_prod_tn))
		#schema = cur.fetchone()

		#entries = [ tmp.strip() for tmp in schema[0].splitlines() if tmp.find("constraint")>=0 or tmp.find("unique")>=0 ]
		#for i in entries: print(i)

		# Test to see if constraints exist
		#cur = conn.cursor()
		#unique_tns = wi_df.columns[:4].text
		#cur.execute('ALTER TABLE {} ADD CONSTRAINT file_api_pool_date_entry UNIQUE ({},{},{},{});'.format(monthly_prod_tn, *unique_tns))
		#cur.close()

		print("Done.\nResults saved in:\n{}".format(path.join(getcwd(), outdir)))
			
	else:
		print("One of (username / password / start_year) did not exist")
		print(username, "/ password not shown /", start_year)