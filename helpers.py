import getpass
from datetime import datetime
from os import path, mkdir, getcwd


# Create a subdirectory when the program starts to hold the data from each scrape.
def create_outdir(today):
	outdir = './ND Oil-Gas Data--Gathered {}'.format(today.strftime("%b-%d-%Y_%H.%M.%S"))
	if not path.exists(outdir):
		mkdir(outdir)
	subdir = path.join(outdir, "by_month")
	if not path.exists(subdir):
		mkdir(subdir)
	return outdir


# A dictionary to make the month value more human readable.
def get_month(month):
	months = {1: 'Jan', 2: 'Feb', 3: 'Mar',
			  4: 'Apr', 5: 'May', 6: 'Jun',
			  7: 'July', 8: 'Aug', 9: 'Sep',
			  10: 'Oct', 11: 'Nov', 12: 'Dec'}
	return months[month]


# Looks ridiculous but the form doesn't use the actual year value so we need to convert it.
# Current year = 1 in the form. So using 2019 as an example we need to subtract 2019 from 2020 (current_year + 1);
def get_year(year):
	current_year = int(datetime.now().year)
	return current_year + 1 - year


# Builds a list of Month/Year pairs. This will be iterated over in the selenium scrape function.
def build_scrape_dates(start_year):
	start_month = 1
	current_year = int(datetime.now().year)
	current_month = int(datetime.now().month)
	scrape_dates = list()

	print("Preparing to Scrape well data from Jan-{} through {}-{}".format(start_year, get_month(current_month), current_year))

	while start_year <= current_year:
		while start_month <= 12:
			if start_year == current_year and start_month > current_month:
				break
			scrape_dates.append([start_month, start_year])
			start_month += 1
		start_month = 1
		start_year += 1
	return scrape_dates


# Converts a string to a sql friendly column name (lowercase, no spaces)
def to_sql_friendly(phrases):
	results = list()
	for phrase in phrases:
		results.append(phrase.replace(" ", "_").lower())
	return results


# prompts user for a username
def get_name():
    name = input("username: ")
    return name


# prompts user for a password
def get_pass():
    try: 
        pw = getpass.getpass() 
    except Exception as error: 
        print('Could not get password', error)
    else:
        return pw


# prompts user to input a year (4 digit integer) between current year and 1960.
def get_start_year():
    year = None
    while not year:
        userInput = input("Which is the oldest year to scrape?\nEnter a 4 digit year between now and 1951\n")
        if len(userInput) == 4:
            try:
                val = int(userInput)
                current_year = int(datetime.now().year)
                if val > 1950 and val <= current_year:
                    year = val
                else:
                    print("\nPlease enter a year between 1960 and the current year.")
            except ValueError:
                print("\nUse digits only")
        else:
            print("\nI couldn't read that, it should be 4 digits.")
    return year


# prompts user to input a month (4 digit integer) between current year and 1960.
def get_start_month():
    month = None
    while not month:
        userInput = input("Which month should we start at? (1-12)\n")
        if len(userInput) >= 1 and len(userInput <= 2):
            try:
                val = int(userInput)
                if val >= 1 and val <= 12:
                    month = val
                else:
                    print("\nPlease enter a number between 1 and 12.")
            except ValueError:
                print("\nUse digits only.")
        else:
            print("\nI couldn't read that, it should only be 1 or 2 digits.")
    return month
