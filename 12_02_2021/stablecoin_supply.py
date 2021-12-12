#%%
import csv,json,time
from pycoingecko import CoinGeckoAPI
import pandas as pd
from bs4 import BeautifulSoup as bs
import datetime
import numpy as np
import lxml, html5lib

#install selenium
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from requests import Request, Session
import requests_cache, re
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import subprocess #to extract from clipboard
#%% get URLs of stablecoins on coinmarketcap

options = webdriver.ChromeOptions()
options.add_argument('--incognito')
options.add_argument('headless')
stablecoin_urls=[]
url = "https://coinmarketcap.com/view/stablecoin/"

wd = webdriver.Chrome(options=options)
wd.get(url)
soup = bs(wd.page_source, 'lxml')
tables = soup.find_all('table')
rows = soup.find_all("tr")
for i in range(1,len(rows)):#ignore first row which is title, and last row
    try:
        links = rows[i].find("a").get("href") # the first link of each row is the coingecko token url that I want to extract
    except:
        links=None
        print("fake row")

    if links!=None:
        #link = links[0]
        stablecoin_urls.append(links)

# %%
remove = ['/currencies/wrapped-bitcoin/','/currencies/reserve-rights/','/currencies/qcash/',
        '/currencies/steem-dollars/', '/currencies/stasis-euro/', '/currencies/usdx-kava/',
        '/currencies/digixdao/','/currencies/xsgd/', '/currencies/digix-gold-token/',
 '/currencies/bitcny/',
 '/currencies/rupiah-token/',
 '/currencies/xaurum/',
 '/currencies/nubits/',
  '/currencies/cryptofranc/',   '/currencies/hellogold/','/currencies/italian-lira/',  '/currencies/midas-dollar-share/',
 '/currencies/meter-stable/',
 '/currencies/binance-vnd/']

for i in remove:
    stablecoin_urls.remove(i)

top15 = stablecoin_urls[0:15]


# %%
#%% historical data for each token

all_url = top15

options = webdriver.ChromeOptions()
options.add_argument('--incognito')
options.add_argument('headless')

for i in range(len(all_url)): #len(all_url)
    print(i)
    url = "https://coinmarketcap.com" + all_url[i] + "historical-data/"

    wd = webdriver.Chrome(options=options)
    wd.get(url)
    time.sleep(1)

    # #click "date range" button
    button_class = "sc-fznKkj.fNvBme"
    #button_class = "icon-Chevron-left" 
    try:
        WebDriverWait(wd,10).until(EC.presence_of_element_located((By.CLASS_NAME,button_class)))
    except:
        print("Cannot find 'Date range' button!")

    dateRange_button = wd.find_element_by_class_name(button_class)
    wd.execute_script("arguments[0].click();", dateRange_button)

    time.sleep(1)

    ## select start date: Jan 1, 2015
    # click on the header twice
    header_class = "pickerHeader___2brSa"
    header = wd.find_element_by_class_name(header_class)

    spans = header.find_elements_by_tag_name('span')
    wd.execute_script("arguments[0].click();", spans[1])
    time.sleep(1)
    wd.execute_script("arguments[0].click();", spans[1])

    #select year
    year_class = "yearpicker.show"
    year = wd.find_element_by_class_name(year_class)
    yearList = year.find_elements_by_tag_name('span') #get all the <span> tags
    wd.execute_script("arguments[0].click();", yearList[0]) #the first span tag is 2015
    time.sleep(1)

    # #select month
    month_class = "monthpicker.show"
    month = wd.find_element_by_class_name(month_class)
    monthList = month.find_elements_by_tag_name('span') #get all the <span> tags
    wd.execute_script("arguments[0].click();", monthList[0]) #the first span tag is 2015

    #select day
    date_class = "react-datepicker__day.react-datepicker__day--001"
    date = wd.find_element_by_class_name(date_class)
    wd.execute_script("arguments[0].click();", date)

    # select end date: March 2, 2021
    header_class = "pickerHeader___2brSa"
    header2 = wd.find_element_by_class_name(header_class)

    spans2 = header2.find_elements_by_tag_name('span')
    wd.execute_script("arguments[0].click();", spans[1])
    time.sleep(1)
    wd.execute_script("arguments[0].click();", spans[1])
    time.sleep(1)

    right_arrow = "icon-Chevron-right "
    arrow = wd.find_element_by_class_name(right_arrow)
    wd.execute_script("arguments[0].click();", arrow)
    time.sleep(1)

    year_class = "yearpicker.show"
    year2 = wd.find_element_by_class_name(year_class)
    yearList2 = year.find_elements_by_tag_name('span') #get all the <span> tags
    wd.execute_script("arguments[0].click();", yearList2[6]) #the first span tag is 2015

    month_class = "monthpicker.show"
    month2 = wd.find_element_by_class_name(month_class)
    monthList2 = month.find_elements_by_tag_name('span') #get all the <span> tags
    wd.execute_script("arguments[0].click();", monthList2[3]) #the first span tag is 2015
    time.sleep(1)

    date_class2 = "react-datepicker__day.react-datepicker__day--015"
    date2 = wd.find_element_by_class_name(date_class2)
    wd.execute_script("arguments[0].click();", date2)

    time.sleep(1)
    #click the "Continue" button
    continue_class = "sc-fznKkj.bzYOzd"
    continue_button = wd.find_element_by_class_name(continue_class)
    wd.execute_script("arguments[0].click();", continue_button)

    time.sleep(8)
    
    #%%
    soup = bs(wd.page_source, 'lxml')
    tables = soup.find_all('table')

    df = pd.read_html(str(tables))[0] #extract entire table
    df['url'] = all_url[i]

    if i == 0:
        stable_market = df
    else:
        stable_market = stable_market.append(df, ignore_index=True)

    wd.close()
top15_stable = stable_market[stable_market["url"].isin(top15)]
#top15_stable = top15.sta
var1 = ['Market Cap','Close**']
for var in var1:
    print(var)
    top15_stable[var] = top15_stable[var].str.replace(',', '')
    top15_stable[var] = top15_stable[var].str.replace('$', '')
    top15_stable[var] = top15_stable[var].astype('float')


top15_stable["supply"] = top15_stable["Market Cap"]/top15_stable["Close**"]
top15_stable.to_csv("stable15_market.csv")
# %%
top15_mc = top15_stable.pivot_table(index=["Date"], columns="url",values="Market Cap").reset_index()
top15_mc.to_csv("top15_mc.csv")
top15_supply = top15_stable.pivot_table(index=["Date"], columns="url",values="supply").reset_index()
top15_supply.to_csv("top15_supply.csv")

# %%
