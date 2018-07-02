import requests
from bs4 import BeautifulSoup
import pandas as pd
import math


list_comp = ['TCS', 'Reliance', 'HDFC Bank', 'HUL', 'ITC', 'HDFC', 'Infosys', 'Maruti Suzuki', 'Kotak Mahindra', 'SBI', 'ONGC', 'Larsen', 'ICICI Bank', 'Coal India', 'Bharti Airtel', 'IOC', 'Sun Pharma', 'Bajaj Finance', 'NTPC', 'Axis Bank', 'HCL Tech', 'Wipro', 'Asian Paints', 'Hind Zinc', 'IndusInd Bank', 'M&M', 'UltraTechCement', 'Power Grid Corp', 'Nestle', 'Bajaj Finserv', 'Avenue Supermar', 'HDFC Life', 'Vedanta', 'Godrej Consumer', 'Bajaj Auto', 'BPCL', 'JSW Steel', 'Yes Bank', 'Titan Company', 'Eicher Motors', 'Tata Motors', 'Adani Ports', 'GAIL', 'Britannia', 'Hero Motocorp', 'Dabur India', 'Tata Steel', 'SBI Life Insura', 'Grasim', 'Tech Mahindra', 'General Insuran', 'Bandhan Bank', 'Motherson Sumi', 'Bharti Infratel', 'Shree Cements', 'ICICI Prudentia', 'Pidilite Ind', 'Bosch', 'New India Assur', 'Zee Entertain', 'Hindalco', 'Cipla', 'Indiabulls Hsg', 'United Spirits', 'Piramal Enter', 'Marico', 'Interglobe Avi', 'Ambuja Cements', 'Lupin', 'HPCL', 'Cadila Health', 'Biocon', 'Dr Reddys Labs', 'Ashok Leyland', 'Aurobindo Pharm', 'Siemens', 'Oracle Fin Serv', 'SAIL', 'NMDC', 'Havells India', 'DLF', 'Petronet LNG', 'Colgate', 'Bajaj Holdings', 'P and G', 'Container Corp', 'MRF', 'ICICI Lombard', 'UPL', 'Page Industries', 'Sun TV Network', 'United Brewerie', 'L&T Finance', 'Bank of Baroda', 'Shriram Trans', 'Indiabulls Vent', 'AB Capital', 'M&M Financial', 'Future Retail', 'L&T Infotech']


import mysql.connector
from mysql.connector import errorcode

try:
  cnx = mysql.connector.connect(user='root',database='moneycontrol',
                                host = '127.0.0.1')
  print('Running ok')
except mysql.connector.Error as err:
  if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
    print("Something is wrong with your user name or password")
  elif err.errno == errorcode.ER_BAD_DB_ERROR:
    print("Database does not exist")
  else:
    print(err)

cursor = cnx.cursor(buffered=True)
sql_command = """CREATE TABLE IF NOT EXISTS company_data(  
name VARCHAR(200) PRIMARY KEY, 
market_cap_cr REAL, 
pe REAL, 
sector VARCHAR(200));"""
cursor.execute(sql_command)
print('Table done')

# Libraries required to limit the time taken by a request
import signal
from contextlib import contextmanager

baseurl		= "http://www.moneycontrol.com"
company_sector = {}

class TimeoutException(Exception): pass

@contextmanager
def time_limit(seconds):
	def signal_handler(signum, frame):
		raise TimeoutException
	signal.signal(signal.SIGALRM, signal_handler)
	signal.alarm(seconds)
	try:
		yield
	finally:
		signal.alarm(0)



def get_response(aurl):
    content = requests.get(aurl,timeout = 5).content
    return content

# Procedure to return a parseable BeautifulSoup object of a given url
def get_soup(aurl):
	response = get_response(aurl)
	soup = BeautifulSoup(response,'html.parser')
	return soup


def get_categories(aurl):
    soup	= get_soup(aurl)
    print(aurl)
    links = {}
    tables	= soup.find('div',{'class':'lftmenu'})
    categories = tables.find_all('li')
    for category in categories:
        category_name = category.get_text()
        if category.find('a',{'class':'act'}):
            links[category_name] = aurl
        else:
            links[category_name] = baseurl + category.find('a')['href']
    return links


def get_values(soup):
    data	= soup.find_all('div',{'class':'FR gD_12'})
    list_ = []
    for dat in data:
        try:
            x = float(dat.get_text().replace(",", ""))
            list_.append(x)
        except ValueError:
            list_.append(0.0)
    return list_[0:2]


def get_Data(aurl):
    soup	= get_soup(aurl)
    og_table	= soup.find('div',{'id':'mktdet_1'})
    table = og_table
    
    
    return get_values(table)





def get_sector(asoup):
    sector = None
    try:
        details = asoup.find('div',{'class':'PB10'})
        headers = details.get_text().split('|')
    except AttributeError:
        return sector
    for header in headers:
        if "SECTOR" in header:
            sector = header.split(':')[1].strip()
            break
    return sector

def get_Company_Data(aurl):
    soup	= get_soup(aurl)
    temp 		= soup.find('dl',{'id':'slider'})
    try:
        links	= temp.find_all(['dt','dd'])
    except AttributeError:
        print("Data doesn't exist anymore.")
        return [0.0,0.0,get_sector(soup)]
    return get_Data(aurl) + [get_sector(soup)]
	
def get_alpha_quotes(aurl):
    soup = get_soup(aurl)
    list = soup.find('table',{'class':'pcq_tbl MT10'})
    companies = list.find_all('a')
    for company in companies[:]:
        x = company.get_text()
        print(x)
        if x in list_comp:
            print(x+" : "+company['href'])
            temp = [x] + get_Company_Data(company['href'])
            print(type(temp[2]))
            format_str="""INSERT IGNORE INTO company_data(name, market_cap_cr, pe, sector) VALUES ('{name}', '{market_cap_cr}', '{pe}', '{sector}');"""
            sql_command = format_str.format(name = temp[0], market_cap_cr=temp[1], pe=temp[2], sector=temp[3])
            cursor.execute(sql_command)
            print('Execution done')
            cnx.commit()
              

def get_all_quotes_data(aurl):
	soup = get_soup(aurl)
	list = soup.find('div',{'class':'MT2 PA10 brdb4px alph_pagn'})

	links= list.find_all('a')

	for link in links[1:]:
		# print(link.get_text()+" : "+baseurl+link['href'])
		print("Accessing list for : "+link.get_text())
		get_alpha_quotes(baseurl+link['href'])
    

if __name__ == '__main__':
    sector_url		= 'http://www.moneycontrol.com/india/stockmarket/sector-classification/marketstatistics/nse/automotive.html'
    quote_list_url 	= 'http://www.moneycontrol.com/india/stockpricequote'
    url 			= quote_list_url
    print("Initializing")
    get_all_quotes_data(url)
    sql_command = "SELECT * FROM company_data"
    df = pd.read_sql(sql_command, cnx)
    df['pe_bucket'] = math.ceil(df.pe/5)
    grouped_df = df[['name','pe_bucket']].groupby('pe_bucket')

    for key, item in grouped_df:
        print(grouped_df.get_group(key), "\n\n")
    print("3rd and 4th highest market cap companies sector wise")
    print(df[['name','market_cap_cr','sector']].groupby(["sector"],sort = True).nth(3))
    print(df[['name','market_cap_cr','sector']].groupby(["sector"],sort = True).nth(4))

    sql_command = """IF NOT EXISTS( SELECT NULL
            FROM INFORMATION_SCHEMA.COLUMNS
           WHERE table_name = 'company_data'
             AND table_schema = 'moneycontrol'
             AND column_name = 'pe_bucket')  THEN

  ALTER TABLE `company_data` ADD `pe_bucket` int(1);

END IF """
    cursor.execute(sql_command)
    sql_command = """UPDATE company_data
                    SET pe_bucket = 
                    
        CASE
        WHEN pe_bucket IS NULL THEN
            CEILING(pe/5)
        END
        """
    cursor.execute(sql_command)
    cnx.commit()
    cnx.close()