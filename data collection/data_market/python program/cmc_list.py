## scrapes top 2000 tokens from https://coinmarketcap.com/ for:
## row number, cmc url, token name, token symbol, max supply, and current market cap
## save as dataframe and csv

### Scrape the URL of each token for historical market data: date: closing price, volume, marketcap


#pip install pycoingecko
#pip install coinmarketcap

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

save = list(range(0,2000,100)) #save after every 100 tokens scraped
#%%#%% Go to coinmarketcap.com, and extract CMC url for all tokens on pages 1-20
options = webdriver.ChromeOptions()
options.add_argument('--incognito')
options.add_argument('headless')
cmc_urls=[]

for j in range(1,21):
    url = "https://coinmarketcap.com/?page="+str(j)

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
            cmc_urls.append(links)

all_tokens = pd.DataFrame(cmc_urls,columns=["cmc_url"])
print(all_tokens[all_tokens.duplicated()])
all_tokens.to_csv("tokens_cmc.csv", index=False)

#%% historical data for each token
cmc_all = pd.read_csv("tokens_cmc.csv")


all_url = cmc_all["cmc_url"].tolist()
save = list(range(0,2000,100))

options = webdriver.ChromeOptions()
options.add_argument('--incognito')
options.add_argument('headless')

for i in range(1400,len(all_url)): #len(all_url)
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
    wd.execute_script("arguments[0].click();", monthList2[2]) #the first span tag is 2015
    time.sleep(1)

    date_class2 = "react-datepicker__day.react-datepicker__day--002"
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

    if i == 1400:
        cmc_market = df
    else:
        cmc_market = cmc_market.append(df, ignore_index=True)

    if i in save:
        cmc_market.to_csv("cmc_market1.csv")

    wd.close()

cmc_market.to_csv("cmc_market1.csv")
#%% If the CMC market data scrape was done in multiple batches, merge the data files. 
# cmc data saved in two files: cmc_market.csv and cmc_market1.csv
# the last token of cmc_market.csv is the first token of cmc_market1.csv, so need to delete the duplicates and then merge

cmc_market = pd.read_csv("cmc_market.csv",index_col=0)
cmc_market1 = pd.read_csv("cmc_market1.csv",index_col=0)
#find last token of cmc_market
tokens = cmc_market['url'].unique()
# delete the last token of cmc_market from cmc_market1
cmc_market1 = cmc_market1[cmc_market1['url']!=tokens[-1]]
cmc_market1.nunique()
# combine the two by append
cmc_market_all = cmc_market.append(cmc_market1,ignore_index=True)
num_tokens = cmc_market_all.nunique() # number of unqiue urls should be 2000

cmc_market_all = cmc_market_all.drop(['Open*', 'High', 'Low'],axis=1)
cmc_market_all = cmc_market_all.rename(columns={'Date':"date", 'Close**':'close', 'Volume':"volume", 'Market Cap':"market_cap"})

cmc_market_all.to_csv("cmc_market_all.csv", index=False)
#%% The CMC market data is stored as string. Turn them into numbers and dates. 
#make all column names lower case, without special characters
cmc_market_all = pd.read_csv("cmc_market_all.csv")

#turn market variables into float
var1 = ['close', 'volume', 'market_cap']

tiny_values = cmc_market_all[cmc_market_all.isin(["<$0.00000001"]).any(axis=1)]
tiny_values.to_csv("tiny_values.csv",index=False)

cmc_market_all = cmc_market_all.replace("<$0.00000001",np.nan)

for var in var1:
    print(var)
    cmc_market_all[var] = cmc_market_all[var].str.replace(',', '')
    cmc_market_all[var] = cmc_market_all[var].str.replace('$', '')
    cmc_market_all[var] = cmc_market_all[var].astype('float')

#change string dates to datetime
cmc_market_all['date'] = cmc_market_all['date'].str.replace(',', '')
cmc_market_all['date'] = cmc_market_all['date'].str.replace(' ', '')
cmc_market_all['date'] = pd.to_datetime(cmc_market_all['date'],format="%b%d%Y")
cmc_market_all.to_csv("cmc_market_all.csv", index=False)

#%% Scrape list of all  categories
category_page = "https://coinmarketcap.com/cryptocurrency-category/"

options = webdriver.ChromeOptions()
options.add_argument('--incognito')

wd = webdriver.Chrome(options=options)
wd.get(category_page)
soup = bs(wd.page_source, 'lxml')
tables = soup.find_all('table')

categories = pd.read_html(str(tables))[0] #extract entire table

category_url = []
rows = soup.find_all("tr")
for i in range(1,len(rows)):
    links = rows[i].find("a").get("href")
    category_url.append(links)

categories["category_url"] = category_url
categories = categories.drop(["Top Gainers", "Volume","#"], axis=1)
cols = categories.columns.tolist()
cols = cols[-1:]+cols[:-1] #reorder so url i smoved from last to first
categories = categories[cols]

categories.to_csv("categories.csv",ignore_index=true)

#%% Go to each token's page and extract the table of all the market pairs
cmc_all = pd.read_csv("tokens_cmc.csv")
all_url = cmc_all['cmc_url']
options = webdriver.ChromeOptions()
options.add_argument('--incognito')
options.add_argument('--headless')
#html class of the "Load More" button
loadButton_class = "sc-fznKkj.bCpxsi.sc-1arsm3t-6.jBBqnu"

#"found" is indicator ofwether the "load more" button is still available
found = 0 # 0 means button was found, 1 means it was not found so the whole table has been loaded. 

for i in range(200,len(all_url)):
    print(i)
    found==0
    url = "https://coinmarketcap.com" + all_url[i] + "markets/"

    wd = webdriver.Chrome(options=options)
    wd.get(url)

    found = 0
    #find the "Load More" button and click it until it disappears. 
    while found==0:
        try:
            WebDriverWait(wd,8).until(EC.presence_of_element_located((By.CLASS_NAME,loadButton_class)))
            found = 0
            load_button = wd.find_element_by_class_name(loadButton_class)
            wd.execute_script("arguments[0].click();", load_button)
        except:
            found = 1
            print("Cannot find 'Load More' button!")
        time.sleep(0.5)
        print(found)

    soup = bs(wd.page_source, 'lxml')
    tables = soup.find_all('table')

    df = pd.read_html(str(tables))[0] #extract entire table 
    df["url"] = all_url[i]

    if i==200:
        market_pair = df
    else:
        market_pair = market_pair.append(df,ignore_index=True)

    if i in save:
        market_pair.to_csv("market_pair1.csv", index=False)

    wd.close()

#Note: save manually on 03/27/2021
market_pair.to_csv("market_pair1.csv", index=False)

#%% append market_pair1 to market_pair.csv
market_pair = pd.read_csv("market_pair.csv",index_col=0)
market_pair1 = pd.read_csv("market_pair1.csv",index_col=0)
# drop empty columns in market_pair1
for i in range(9):
    market_pair1 = market_pair1.drop(str(i), axis=1)

#drop first token of market_pair1 as it's already in market_pair
tokens1 = market_pair1["url"].unique()
market_pair1 = market_pair1[market_pair1['url']!= tokens1[0]]
market_pair = market_pair.append(market_pair1,ignore_index=True)
market_pair.to_csv("market_pair_all.csv", index=False)

# %% Got to each token's site and scrape:
#    the source code, 
#   white paper
#   tags (categories)
#   contracts (indicate whether there is ethereum contract)
#%% scrape contracts
cmc_all = pd.read_csv("tokens_cmc.csv")
all_url = cmc_all["cmc_url"].tolist()

save = list(range(0,2000,100))

smart_chains = ['Ethereum','Binance Smart Chain','Tron','Solana','Algorand','Heco','Xdai chain','Polygon']
cmc_all[smart_chains] = np.nan

options = webdriver.ChromeOptions()
options.add_argument('--incognito')

for k in range(101,len(all_url)):#len(all_url)
    print(k)
    url = "https://coinmarketcap.com"+all_url[k] 
    print(url)
    wd = webdriver.Chrome(options=options)
    action = ActionChains(wd)
    wd.get(url)

    #Find the "Contracts" section
    try:
        contract_class = "sc-AxhCb.bYwLMj.container___2dCiP.contractsRow"
        WebDriverWait(wd,8).until(EC.presence_of_element_located((By.CLASS_NAME,contract_class)))
        contract_section = wd.find_element_by_class_name(contract_class)

        #find main chain
        main_chain_class = "mainChainTitle___3jIHB"
        main_chain = contract_section.find_element_by_class_name(main_chain_class).text
        clipboard_class = "sc-AxhCb.bRWPvW.externalLinkIcon___2oO2O"
        clipboard2 = contract_section.find_element_by_class_name(clipboard_class)
        clipboard1 = clipboard2.find_element_by_tag_name("path")
        action.move_to_element(clipboard1).click().perform()
        link = subprocess.check_output(["xsel", "--clipboard"]) 
        main_contract = str(link)[2:-1]

        if main_chain in smart_chains:
            cmc_all[main_chain].iloc[k] = main_contract

        #Find the ""More button for the dropdown of all contracts. 
        try:
            more_button_class = "buttonName___3G9lW"
            more_button = contract_section.find_element_by_class_name(more_button_class)
            wd.execute_script("arguments[0].click();", more_button)

            #Find all contracts in the dropdown list
            contract_title_class = "sc-AxjAm.gTzmEg.contractAddressTitle___1FVmp" #contract platform name
            contract_title =  contract_section.find_elements_by_class_name(contract_title_class)
            contract_names = [contract.find_element_by_tag_name("span").text for contract in contract_title]

            clipboard_class = "sc-AxjAm.cITTod.externalLinkIcon___2oO2O"
            clipboard2 = contract_section.find_elements_by_class_name(clipboard_class)
            #clipboard1 = clipboard2.find_element_by_tag_name("path")

            contract_address = []
            for i in range(len(contract_names)):
                # click on the clipboard
                '''
                dropdown_class = "dropdownItem___NSKhL.platformDropdownItem___zuSvE"
                dropdown =  contract_title[i].find_element_by_class_name(dropdown_class)
                clipboard_class = "sc-AxjAm.cITTod.externalLinkIcon___2oO2O"
                clipboard2 = dropdown.find_element_by_class_name(clipboard_class)
                '''
                clipboard1 = clipboard2[i*2].find_element_by_tag_name("path")
                action.move_to_element(clipboard1).click().perform()
                link = subprocess.check_output(["xsel", "--clipboard"]) 
                contract_address.append(str(link)[2:-1])

            contracts = dict(zip(contract_names, contract_address))

            for i in range(len(smart_chains)):
                if smart_chains[i] in contract_names:
                    cmc_all[smart_chains[i]].iloc[k] = contracts.get(smart_chains[i])

        except:
            print("only 1 smart chain platform")
            
    except:
        print("no contract")

    if k in save:
        cmc_all.to_csv("cmc_contract1.csv",index=False)

    wd.close()

cmc_all.to_csv("cmc_contract1.csv",index=False)
#%% Merge the contract files cmc_contract.csv and cmc_contract1.csv
cmc_contract = pd.read_csv("cmc_contract.csv")
cmc_contract1 = pd.read_csv("cmc_contract1.csv")

cmc_contract1.iloc[0:103] = cmc_contract.iloc[0:103]
cmc_contract_all = cmc_contract1

cmc_contract_all.to_csv("cmc_contract_all.csv", index=None)
# %%scrape source code, max supply, tags
'''
cmc_all = pd.read_csv("tokens_cmc.csv")
all_url = cmc_all["cmc_url"].tolist()
category_file = pd.read_csv("categories.csv", index_col=0)
category_list = category_file['Name'].tolist()
category_list.append("Store of Value")
category_list.append("PoW")
category_list.append("Payments")
category_list.append("Medium of Exchange")

cmc_all[['github', 'max_supply',]] = np.nan  # create empty columns for all categories. 
cmc_all[category_list] = np.nan

save = list(range(0,2000,100))

options = webdriver.ChromeOptions()
options.add_argument('--incognito')
'''

for k in range(1255,len(all_url)):
    url = "https://coinmarketcap.com"+ all_url[k]

    wd = webdriver.Chrome(options=options)
    action = ActionChains(wd)
    wd.get(url)

    time.sleep(3)

    #source code
    source_code_section = wd.find_elements_by_class_name("button___2MvNi")
    # find section with source code
    source_code = [section for section in source_code_section if section.text=="Source code"]
    if source_code!=[]:
        git = source_code[0].get_attribute('href')
    else:
        git=np.nan

    cmc_all["github"].iloc[k] = git

    # Find max supply
    find_supply = wd.find_elements_by_class_name("maxSupplyValue___1nBaS")
    if find_supply!=[]:
        max_supply = find_supply[0].text
        if max_supply == "--":
            max_supply = np.nan

    cmc_all["max_supply"].iloc[k] = max_supply

    #Tags
    #find the section of the tags
    tag_section = wd.find_elements_by_class_name("sc-AxhCb.bYwLMj.container___2dCiP")
    tagrow= [section for section in tag_section if "Tags" in section.text]
    if tagrow!=[]:
        lis = tagrow[0].find_elements_by_tag_name("li")
        view_all = [li for li in lis if "View all" in li.text] #find the "View all button"#

        if view_all != []: #click on 'View all' if there are more than 4 tags
            view_all[0].click()
            time.sleep(2)

            #find the tags block that appears
            tagblock = wd.find_element_by_class_name("sc-AxhCb.bYooYf")
            tag_class = tagblock.find_elements_by_class_name("tagBadge___3p_Pk")
            tags = [tag.text for tag in tag_class]
            for i in tags:
                if i in category_list:
                    cmc_all[i].iloc[k] = 1
        else: 
            tags = [tag.text for tag in lis]
            for i in tags:
                if i in category_list:
                    cmc_all[i].iloc[k] = 1

    if k in save:
        cmc_all.to_csv('cmc_all_tags1.csv',index = False)
    wd.close()

cmc_all.to_csv('cmc_all_tags1.csv',index = False)
# %% Some scraped github urls are wrong,replace with correct ones manually

cmc_all = pd.read_csv("cmc_all_tags1.csv")
cmc_all.to_csv("cmc_all_tags.csv")

# %%Group the categories

#drop tokens without instrinsic value: ie pegged to something else. 
cmc_all = pd.read_csv("cmc_all_tags1.csv") # should I be using cmc_all_tags.csv instead?
indexnames = cmc_all[(cmc_all['Stablecoin']==1) | (cmc_all["Synthetics"]==1)|(cmc_all["Tokenized Stock"]==1 )
                   | (cmc_all['Wrapped Tokens']==1) | (cmc_all['ETH 2.0 Staking']==1)
                   | (cmc_all['ETH 2.0 Staking']==1)].index
cmc_all.drop(indexnames,inplace=True)

#defi combines: DeFi, Yield Farming, AMM, Oracles, Lending/borrowing, derivatives, yield aggregator, insurance, rebase, seigniorage, options, Defi index,DEX, yearn partnership
cmc_all['defi'] = np.where((cmc_all["DeFi"]==1 )|(cmc_all["Yield farming"]==1) |(cmc_all["AMM"]==1)|(cmc_all["Lending / Borrowing"]==1)
                |(cmc_all["Derivatives"]==1)|(cmc_all["Yield Aggregator"]==1)|(cmc_all["Insurance"]==1)
                | (cmc_all["Rebase"]==1) |(cmc_all["Seigniorage"]==1) |(cmc_all["Options"]==1)
                | (cmc_all["DeFi Index"]==1) | (cmc_all["Decentralized exchange"]==1) | (cmc_all["Yearn Partnerships"]==1),1, np.nan)

# distributed_comp combines: Filesharing, storage, distributed computing
cmc_all["distributed_comp"] = np.where((cmc_all["Filesharing"]==1 )|(cmc_all["Storage"]==1 )|(cmc_all["Distributed Computing"]==1),1,np.nan)

#entertainment combines: Content Creation, Media, Memes, Videos, entertainment, music, fan token, 
#                       communication and social media, events, social money, gaming, sports, gambling
cmc_all["entertainment"] = np.where((cmc_all["Content Creation"]==1)|(cmc_all["Media"]==1)|(cmc_all["Memes"]==1)|
                        (cmc_all["Video"]==1)|(cmc_all["Entertainment"]==1)| (cmc_all["Music"]==1)|
                        (cmc_all["Fan token"]==1)|(cmc_all["Communications & Social Media"]==1)|(cmc_all["Events"]==1)
                        |(cmc_all["Social Money"]==1)|(cmc_all["Gaming"]==1)|(cmc_all["Sports"]==1)|(cmc_all["Gambling"]==1)|(cmc_all["NFTs & Collectibles"]==1),1,np.nan)

#infrastructure combines scaling, interoperability, smart contract platform
cmc_all["infrastructure"] = np.where((cmc_all["Scaling"]==1)|(cmc_all["Interoperability"]==1)|(cmc_all["Smart Contracts"]==1),1,np.nan)

cmc_all = cmc_all.rename(columns = {"Centralized exchange": "cex", "Privacy":"privacy","Asset management":"asset_management"})
cmc_all["biz_solution"] = np.where((cmc_all["Identity"]==1)|(cmc_all["Analytics"]==1)|(cmc_all["Marketing"]==1)|
                            (cmc_all["Logistics"]==1),1,np.nan)

#drop categories that have been combined together. 
cmc_all = cmc_all.drop(["DeFi","Yield farming","AMM","Lending / Borrowing","Derivatives","Yield Aggregator"
                        ,"Insurance","Rebase","Seigniorage","Options","DeFi Index","Decentralized exchange"
                        ,"Yearn Partnerships","Content Creation","Media","Memes","Video","Entertainment","Music"
                        ,"Fan token","Communications & Social Media","Events","Social Money","Gaming","Sports"
                        ,"Gambling","NFTs & Collectibles","Scaling","Interoperability","Smart Contracts","Identity"
                        ,"Analytics","Marketing","Logistics"], axis=1)
# %%
cmc_all.to_csv("cmc_industry_groups.csv", index=False)
# %% merge cmc_market_all.csv with cmc_industry_groups.csv and cmc_contract_all.csv
cmc_contract = pd.read_csv("cmc_contract_all.csv")
industries = pd.read_csv("cmc_industry_groups.csv")
cmc_market = pd.read_csv("cmc_market_all.csv")
cmc_market = cmc_market.rename(columns = {"url":"cmc_url"})
contract_industries = cmc_contract.merge(industries, how="outer", on=["cmc_url"])
market_data = contract_industries.merge(cmc_market,how="outer",on=["cmc_url"])
market_data.to_csv("market_data.csv",index=False)

# %%
