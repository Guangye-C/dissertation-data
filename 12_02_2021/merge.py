#merge market_data.csv with onchain_data.csv by token_address and date

#%%
import pandas as pd
import numpy as np
import datetime
import csv,json,time

#merge cmc_all.csv with onchain_data.csv, then create industry groups and drop some categories. 
cmc_all = pd.read_csv("cmc_all_tags1.csv")
cmc_contract = pd.read_csv("cmc_contract_all.csv")
#industries = pd.read_csv("cmc_industry_groups.csv")
cmc_market = pd.read_csv("cmc_market_all.csv")

#%% drop duplicated cmc_contract
A = cmc_contract[cmc_contract["Ethereum"].notnull()]
dupes = A[A["Ethereum"].duplicated()]["cmc_url"]
dupes=dupes.tolist()
cmc_contract = cmc_contract[~cmc_contract["cmc_url"].isin(dupes)]


#%% merge contracts with tag by cmc_url, then merge with onchain by token_address
# lastly with market data by cmc_url
#contract_tags = cmc_contract.merge(cmc_all, how="outer", on=["cmc_url"])
#contract_tags= contract_tags.rename(columns = {"Ethereum": "token_address"})
#contract_tags["token_address"] = contract_tags['token_address'].str.lower() 
cmc_contract= cmc_contract.rename(columns = {"Ethereum": "token_address"})
cmc_contract["token_address"] = cmc_contract['token_address'].str.lower() 
onchain = pd.read_csv("onchain_data.csv")
onchain["date"] = pd.to_datetime(onchain['date'])
#contract_tags["date"] = pd.to_datetime(contract_tags['date'])
#onchain_tags = onchain.merge(contract_tags, how = "outer", on=["token_address"])

onchain_contract = onchain.merge(cmc_contract, how = "outer", on=["token_address"])
#%%
#There were two tokens that was not in onchain
onchain_token_list=  onchain.token_address.unique().tolist()
contract_tags_list = contract_tags.token_address.unique().tolist()
missing2 = [i for i in contract_tags_list if i not in onchain_token_list]

#%%
cmc_market = cmc_market.rename(columns = {"url":"cmc_url"})
cmc_market["date"] = pd.to_datetime(cmc_market["date"])
cmc_market = cmc_market.merge(cmc_all,how="outer", on=["cmc_url"])
onchain_market= onchain_contract.merge(cmc_market,how="outer",on=["cmc_url","date"])
onchain_market = onchain_market.loc[onchain_market.date.notnull()]


#%% for some erc20 tokens, fill in token_address
df = onchain_market
df["token_address"] = df.groupby("cmc_url")["token_address"].ffill()
df["token_address"] = df.groupby("cmc_url")["token_address"].bfill()

#%% fill in the blanks for dummy variables github and afterward
cols = df.columns.tolist()
start = cols.index("github")
for i in range(start,len(cols)):
    df[cols[i]]= df.groupby("cmc_url")[cols[i]].ffill()
    df[cols[i]]= df.groupby("cmc_url")[cols[i]].bfill()

#%%
cmc_all = df

#%% #drop tokens without instrinsic value: ie pegged to something else. 
'''
indexnames = cmc_all[(cmc_all['Stablecoin']==1) | (cmc_all["Synthetics"]==1)|(cmc_all["Tokenized Stock"]==1 )
                   | (cmc_all['Wrapped Tokens']==1) | (cmc_all['ETH 2.0 Staking']==1)].index
cmc_all = cmc_all.drop(indexnames,axis=0)
'''
#defi combines: DeFi, Yield Farming, AMM, Oracles, Lending/borrowing, derivatives, yield aggregator, insurance, rebase, seigniorage, options, Defi index,DEX, yearn partnership
cmc_all['defi'] = np.where((cmc_all["DeFi"]==1 )|(cmc_all["Yield farming"]==1) |(cmc_all["AMM"]==1)|(cmc_all["Lending / Borrowing"]==1)
                |(cmc_all["Derivatives"]==1)|(cmc_all["Yield Aggregator"]==1)|(cmc_all["Insurance"]==1)
                |(cmc_all["Rebase"]==1) |(cmc_all["Seigniorage"]==1) |(cmc_all["Options"]==1)
                |(cmc_all["DeFi Index"]==1) | (cmc_all["Decentralized exchange"]==1) | (cmc_all["Yearn Partnerships"]==1)
                |(cmc_all['Stablecoin']==1) | (cmc_all["Synthetics"]==1)|(cmc_all["Tokenized Stock"]==1 )
                   | (cmc_all['Wrapped Tokens']==1),1, np.nan)

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
 # Add the below tokens to the drop step above if creating dataset without stablecoins
 #, "Stablecoin","Synthetics","Tokenized Stock"
                        #,'Wrapped Tokens','ETH 2.0 Staking'                       
# %% Save final data file
cmc_all.to_csv("onchain_market.csv", index=False)

# %%
onchain_market = pd.read_csv("onchain_market.csv")
df = onchain_market
df["date"] = pd.to_datetime(df["date"])
# %%
market_price = pd.read_csv("/home/guangye/token_empirics/data_collection/March_2021/market_data/cmc_market_all.csv")
# %%
A = market_data[market_data["cmc_url"] =="/currencies/the-midas-touch-gold/"]
# %%
