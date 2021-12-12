
## scrapes top 2000 tokens from https://www.coingecko.com/en/coins/all for:
## row number, coingecko url, total supply, and current market cap
## save as dataframe and csv

#pip install pycoingecko
#pip install coinmarketcap

#%%
import csv,json,time
from pycoingecko import CoinGeckoAPI
import pandas as pd
from bs4 import BeautifulSoup as bs
import datetime
import numpy as np

#install selenium
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from requests import Request, Session
import requests_cache, re
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects

#from coinmarketcapapi import CoinMarketCapAPI, CoinMarketCapAPIError

#%% go to https://www.coingecko.com/en/coins/all and click the "Show More" button 6 times

options = webdriver.ChromeOptions()
options.add_argument('--incognito')

url_cg_all = "https://www.coingecko.com/en/coins/all"
wd = webdriver.Chrome(options=options)
wd.get(url_cg_all)

#wait for the "Show More" button to appear
try:
    WebDriverWait(wd,10).until(EC.presence_of_element_located((By.XPATH,"/html/body/div[3]/div[2]/div[2]/div/a")))
except:
    print("Cannot find 'Show More' button!")

showMorebutton = wd.find_element_by_xpath("/html/body/div[3]/div[2]/div[2]/div/a")

#click "show more" button 6 times to load 2100 tokens
i=0
while i<6:
    showMorebutton.click()
    time.sleep(5)
    i+=1

#%%
soup = bs(wd.page_source, 'lxml')
tables = soup.find_all('table')
dfs = pd.read_html(str(tables)) #dfs is a list with a single element that is the table. 
all_tokens = dfs[0] #extract the table hidden in the list



'''
B = soup.find_all("a",{"data-target":"currency.currencyLink"})
cg_link = [row.get("href") for row in B]

'''
cg_urls=[]
rows = soup.find_all("tr")
for i in range(1,len(rows)):#ignore first row which is title
    link = rows[i].find("a").get("href") # the first link of each row is the coingecko token url that I want to extract
    cg_urls.append(link)

all_tokens["cg_url"] = cg_urls
#%%
all_tokens.to_csv('cg_all_03_13_2021.csv')


#%% get all tags / categroies
site="https://www.coingecko.com/en/categories"
options = webdriver.ChromeOptions()
options.add_argument('--incognito')

wd = webdriver.Chrome(options=options)
wd.get(site)

soup = bs(wd.page_source, 'lxml')
categories=[]
rows = soup.find_all("tr")
for i in range(1,len(rows)):#ignore first row which is title
    catogory_name= rows[i].find("a").get("href") # the first link of each row is the coingecko token url that I want to extract
    categories.append(catogory_name)

categories.append("/en/categories/cryptocurrency")
categories.append("/en/categories/smart-contract-platform")

with open("categories.txt", "w") as fp:
    json.dump(categories, fp)


#smart contract platforms
platforms=['https://assets.coingecko.com/coins/images/4128/small/RPU3hzmh_400x400.jpg?1586762168',
'https://assets.coingecko.com/coins/images/279/small/ethereum.png?1595348880',
'https://assets.coingecko.com/coins/images/4713/small/matic___polygon.jpg?1612939050',
'https://assets.coingecko.com/coins/images/2822/small/huobi-token-logo.png?1547036992',
'https://assets.coingecko.com/coins/images/825/small/binance-coin-logo.png?1547034615',
'https://assets.coingecko.com/coins/images/11062/small/xdai.png?1614727492']

with open("platforms.txt", "w") as fp:
    json.dump(platforms, fp)



#%% Load data

with open("categories.txt", "r") as fp:
    categories = json.load(fp)

with open("platforms.txt", "r") as fp:
    platforms = json.load(fp)

all_tokens = pd.read_csv('cg_all_03_13_2021.csv',index_col=None)
all_tokens = all_tokens.drop(columns="Unnamed: 0")

#create tag and contract variables in all_tokens dataframe
for i in categories:
    all_tokens[i] = np.nan

all_tokens["github"] = np.nan

for i in platforms:
    all_tokens[i] = np.nan

#%% find the tags for each token
## find the github url for each token
## find the contract addresses for for each token, for all platforms that the token is on. 
options = webdriver.ChromeOptions()
options.add_argument('--incognito')
options.add_argument('headless')

for row in range(950,len(all_tokens)):
    print(row)
    site = "https://www.coingecko.com"+all_tokens.loc[row,'cg_url']
    print(site)

    wd = webdriver.Chrome(options=options)
    wd.get(site)
    time.sleep(1)

    soup = bs(wd.page_source, 'lxml')
    #Find all the tags
    tag_links = soup.find_all('a', href=re.compile('^/en/categories/'))
    if tag_links!=None:
        tags = [tag.get("href") for tag in tag_links]    
        for cat in categories:
            if cat in tags:
                all_tokens.loc[row, cat]=1

    #scrape the github url
    git = soup.find('a', href=re.compile('^https://github.com/'))
    if git!=None:
        git_text = git.get('href')
        all_tokens.loc[row,"github"] = git_text

    #%% scrape contracts. FOr each token, each contract address is preceded by a picture icon of that platform. 
    #dropDownbutton = wd.find_element_by_xpath("/html/body/div[4]/div[4]/div[4]/div[1]/div/div[4]/div/div[2]/span")
    #dropDownbutton.click()

    try:
        dropDownbutton = wd.find_element_by_id("dropdownMenuButton")
        dropDownbutton.click()   
    except:
        print("no drop down")

    time.sleep(1)       
    contract_blocks =  wd.find_elements_by_class_name("d-flex.p-2.px-4.justify-content-between.border-bottom") #Find the elements that contain the contracts
    src = [img.find_element_by_css_selector('img').get_attribute('src') for img in contract_blocks] #for each element, find the image link        #print(src)
    contracts = [img.find_element_by_css_selector('i').get_attribute('data-address') for img in contract_blocks] #for each element find the contract address, most will be empty
    #print(contracts)
    for j in range(len(src)):
        if src[j] in platforms:
            all_tokens.loc[row,src[j]] = contracts[j]


    single_contract = wd.find_elements_by_class_name("coin-tag.align-middle")


    src = [img.find_element_by_css_selector('img').get_attribute('src') for img in single_contract] #for each element, find the image link
    #print(src)
    contracts = [img.find_element_by_css_selector('i').get_attribute('data-address') for img in single_contract] #for each element find the contract address, most will be empty
    #print(contracts)
    for j in range(len(src)):
        if src[j] in platforms:
            all_tokens.loc[row,src[j]] = contracts[j]

all_tokens.to_csv("all_tokens_tags1.csv")
#%%