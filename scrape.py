from selenium import webdriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.keys import Keys
from bs4 import BeautifulSoup
from datetime import datetime
from helpers import to_sql_friendly, get_name, get_pass, get_start_year, get_start_month, build_scrape_dates, get_year, get_month, create_outdir
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


def selenium_scrape(date):

	# Set up variables for query
	form_month_num = date[0]
	form_year_num = get_year(date[1])
	date_text = "{}-{}".format(get_month(date[0]), date[1])
	tries = 0

	print("Beginning scrape for month of {}".format(date_text))

	# Set up Selenium
	options = webdriver.ChromeOptions()
	options.add_argument('--headless')
	options.add_argument('--disable-gpu')
	options.add_argument('--no-sandbox')
	capa = DesiredCapabilities.CHROME
	capa["pageLoadStrategy"] = "none"

	while True:
		try:
			# Initialize the driver
			driver = webdriver.Chrome(executable_path='./chromedriver', desired_capabilities=capa, options=options)
			
			# Set up wait time
			wait = WebDriverWait(driver, 10)
			
			# Get the web-pae
			driver.get('https://{}:{}@www.dmr.nd.gov/oilgas/feeservices/stateprod.asp'.format(username, password))
			
			# Wait for the page to load
			wait.until(EC.presence_of_element_located((By.XPATH, '//*[@id="month"]')))

			# Select the month (1 = Jan -- 12 = Dec)
			driver.find_element_by_xpath(f'//*[@id="month"]/option[{form_month_num}]').click()
			# Select the year (1 = 2019 -- 69 = 1951)
			driver.find_element_by_xpath(f'//*[@id="year"]/option[{form_year_num}]').click()
			# Click the button to query the data
			driver.find_element_by_xpath(f'/html/body/table/tbody/tr/td[2]/form/p[2]/input').click()
			
			# Wait for all the table to load
			time.sleep(20)

			# Use Beautiful Soup to get the HTML table
			soup = BeautifulSoup(driver.page_source, 'html.parser')
			table = soup.find(lambda tag: tag.name=='table' and tag.has_attr('id') and tag['id']=='largeTableOutput')
			if table:
				table_rows = table.find_all(lambda tag: tag.name=='tr')
			else:
				print("No data for month of {}".format(date_text))
				driver.execute_script("window.stop();")
				driver.quit()
				return
				

			# Parse the table rows
			df = parse_table(table_rows, form_month_num, form_year_num)
			print("{} records found for {}".format(len(df), date_text))

			#close the webdriver
			driver.execute_script("window.stop();")
			driver.quit()

			return df

		except TimeoutException:
			tries += 1
			# Try the scrape 5 times before failing
			if tries < 5:
				time.sleep(5)
				continue
		break


def parse_table(rows, month, year):
	# Get rows as a list of lists
	data = [list(row.stripped_strings) for row in rows]

	# Load table to pandas dataframe
	df = pd.DataFrame.from_records(data[1:])
	df.columns = data[0]
	
	# Name file (containing one month of data)
	outname = '{}-{:02d}.csv'.format(get_year(year), month)
	fullname = path.join(outdir, "by_month", outname)

	# Save to CSV
	df.to_csv(fullname, index=False)
	return df


if __name__ == "__main__":
	# Global variables
	outdir = None
	start_year = None
	today = datetime.now()
	well_index_url = 'https://www.dmr.nd.gov/oilgas/feeservices/flatfiles/Well_Index.zip'

	if (path.exists("credentials.py")):
		from credentials import username, password
	else:
		username = None  
		password = None
		get_name_and_pw()

	start_year = get_start_year()
	instance_count = 15
	
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
		#with zipfile.ZipFile(wi_bytes) as wi_zip:
		#	wi_zip.extractall(outdir)

		scrape_dates = build_scrape_dates(start_year)
		print("Scraping monthly production data with {} instances.".format(instance_count))
		p = Pool(instance_count)
		monthly_dfs = p.map(selenium_scrape, scrape_dates)
		p.terminate()
		p.join()

		print("Aggregating monthly production data.")
		all_monthly_prod = pd.concat(monthly_dfs, axis=0)
		all_monthly_prod.columns = to_sql_friendly(all_monthly_prod.columns)
		results_name = "Well Production Data for {}-{} to {}-{}".format(get_month(1), start_year, get_month(today.month), today.year)
		all_monthly_prod.to_csv(path.join(outdir, "Monthly Production Aggregated.csv"), index=False)

		print("Building MONTHLY_PRODUCTION SQLite3 table.")
		sqlite_filename = path.join(outdir, "results.sqlite")
		conn = sqlite3.connect(sqlite_filename)
		all_monthly_prod.to_sql('MONTHLY_PRODUCTION', conn, if_exists='append', index=False)

		print("Building WELL_INDEX SQLite3 table.")
		wi_df = pd.read_csv(BytesIO(unzipped[0]))
		#wi_df_b = pd.read_csv(wi_zip.open('WellIndex.csv'))
		wi_df.columns = to_sql_friendly(wi_df.columns)
		wi_df.to_sql('WELL_INDEX', conn, if_exists='append', index=False)

		# print("Updating master SQLite3 DB.")
		conn.close()

		print("Done.\nResults saved in:\n{}".format(path.join(getcwd(), outdir)))
			
	else:
		print("One of (username / password / start_year) did not exist")
		print(username, "/ password not shown /", start_year)