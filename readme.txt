Scraps data from Money Control to do some analysis using Pandas and SQL

Beautiful Soup for scraping Data
mysql-connector for SQL functionalities

A list of companies is given for which the following values are retrieved:
name : Name of the company
sector : Company Sector (as mentioned on moneycontrol.com)
pe : p/e ratio
market_cap_cr: Market Capitilisation in Crores

Once the above fields are extracted new column named pe_bucket is created 
with 1 for 1-5, 5-10 is 2 and so on.

