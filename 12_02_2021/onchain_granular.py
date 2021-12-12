# Look at the trading activity and overlap of the largest bulls and bears
# distinguish users from speculators
# distinguish market making across exchanges from those on the same exchange

#%%
import pandas as pd
import numpy as np
import datetime
from google.cloud import bigquery
import os

#in shell session, enter:
#set GOOGLE_APPLICATION_CREDENTIALS="C:/Users/guangye/Dropbox (University of Michigan)/Dissertation Data/From VSCODE/credentials/crypto-market-making-a6daf70b0eef.json"

#%% Get list of non-stablecoin tokens, including only tokens with more than 180 days of market and onchain data.
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="C:/Users/guangye/Dropbox (University of Michigan)/Dissertation Data/From VSCODE/credentials/crypto-market-making-a6daf70b0eef.json"


df = pd.read_csv("onchain_chg_stata2.csv")
nonstable_contract = pd.DataFrame(df.token_address.unique(), columns=["contract"])
nonstable_contract.to_csv("nonstable_contract.csv", index=False)

#%% List the addresses of the bulls and bears, which the addresses that purchased or sold more than 0.1% of total traded volumne

nonstable_contracts = pd.read_csv("nonstable_contract.csv")
#%% create string that repeats "token_address = A OR token_address = B OR........."
contracts = nonstable_contracts["contract"].dropna().str.lower()

nonstable_contract = ""

for contract in contracts:
    nonstable_contract= f"{nonstable_contract} token_address = '{contract}' OR"   

nonstable_contract = nonstable_contract[:len(nonstable_contract)-3]
#print(allContract)

file = open('nonstable_token_address.txt',"w")
file.write(nonstable_contract)
file.close()

# %%
file = open('nonstable_token_address.txt',"r")
allContract = file.read()
# %% Query Sellers

client = bigquery.Client()

QUERY = (
'''
with 
token_decimals as (
    select address as token_address, cast(decimals as int64) as decimals
    from `bigquery-public-data.crypto_ethereum.tokens`
)

,double_entry_buy as (
    -- debits
    select to_address as to_address, sum(CAST(value AS float64)) as purchase_value, date(block_timestamp) as date, count(value) as num_buy_transfers, token_address
    from `bigquery-public-data.crypto_ethereum.token_transfers`
    where ''' +allContract+ '''
    group by token_address, to_address, date
)

,double_entry_sell as (
    -- credits
    select from_address as from_address, sum(CAST(value AS float64)) as sold_value, date(block_timestamp) as date, count(value) as num_sell_transfers, token_address
    from `bigquery-public-data.crypto_ethereum.token_transfers`
    where ''' +allContract +'''
    group by token_address, from_address, date 
)

,transaction_sum as (
    select sum(purchase_value) as trade_total,date, token_address
    from double_entry_buy 
    group by token_address, date
)

,buy_shares as(
    select safe_divide(purchase_value, trade_total) as purchase_share, transaction_sum.date, transaction_sum.token_address as token_address, to_address, purchase_value, num_buy_transfers,
    row_number() over (partition by transaction_sum.token_address, double_entry_buy.date order by purchase_value desc) as buy_rank
    from double_entry_buy 
    join transaction_sum on double_entry_buy.date = transaction_sum.date and double_entry_buy.token_address = transaction_sum.token_address
)

,sell_shares as(
    select safe_divide(sold_value, trade_total) as sold_share, transaction_sum.date, transaction_sum.token_address as token_address, from_address, sold_value, num_sell_transfers,
    row_number() over (partition by transaction_sum.token_address, double_entry_sell.date order by sold_value desc) as sell_rank
    from double_entry_sell
    join transaction_sum on double_entry_sell.date = transaction_sum.date and double_entry_sell.token_address = transaction_sum.token_address
)

select *
from sell_shares 
order by sell_shares.token_address, sell_shares.date

'''
)
query_job = client.query(QUERY)  # API request
rows = query_job.result()  # Waits for query to finish
print('finished query')

bq_sellers = client.query(QUERY).to_dataframe(create_bqstorage_client=True)

bq_sellers.to_csv("bq_sellers.csv",index=False)

print('finished saving bq_sellers.csv')

# %% Query Buyers

client = bigquery.Client()

QUERY2 = (
'''
with 
token_decimals as (
    select address as token_address, cast(decimals as int64) as decimals
    from `bigquery-public-data.crypto_ethereum.tokens`
)

,double_entry_buy as (
    -- debits
    select to_address as to_address, sum(CAST(value AS float64)) as purchase_value, date(block_timestamp) as date, count(value) as num_buy_transfers, token_address
    from `bigquery-public-data.crypto_ethereum.token_transfers`
    where ''' +allContract+ '''
    group by token_address, to_address, date
)

,double_entry_sell as (
    -- credits
    select from_address as from_address, sum(CAST(value AS float64)) as sold_value, date(block_timestamp) as date, count(value) as num_sell_transfers, token_address
    from `bigquery-public-data.crypto_ethereum.token_transfers`
    where ''' +allContract +'''
    group by token_address, from_address, date 
)

,transaction_sum as (
    select sum(purchase_value) as trade_total,date, token_address
    from double_entry_buy 
    group by token_address, date
)

,buy_shares as(
    select safe_divide(purchase_value, trade_total) as purchase_share, transaction_sum.date, transaction_sum.token_address as token_address, to_address, purchase_value, num_buy_transfers,
    row_number() over (partition by transaction_sum.token_address, double_entry_buy.date order by purchase_value desc) as buy_rank
    from double_entry_buy 
    join transaction_sum on double_entry_buy.date = transaction_sum.date and double_entry_buy.token_address = transaction_sum.token_address
)

,sell_shares as(
    select safe_divide(sold_value, trade_total) as sold_share, transaction_sum.date, transaction_sum.token_address as token_address, from_address, sold_value, num_sell_transfers,
    row_number() over (partition by transaction_sum.token_address, double_entry_sell.date order by sold_value desc) as sell_rank
    from double_entry_sell
    join transaction_sum on double_entry_sell.date = transaction_sum.date and double_entry_sell.token_address = transaction_sum.token_address
)

select *
from buy_shares 
order by buy_shares.token_address, buy_shares.date

'''
)
query_job = client.query(QUERY)  # API request
rows = query_job.result()  # Waits for query to finish
print('finished query')

bq_buyers = client.query(QUERY2).to_dataframe(create_bqstorage_client=True)

bq_buyers.to_csv("bq_buyers.csv",index=False)

print('finished saving bq_buyers.csv')
#%%
sellers = pd.read_csv("bq_sellers.csv")
buyers = pd.read_csv("bq_buyers.csv")

#%% for each token-day, calculate the % of overlap between buyers and sellers: intersection / union

def overlap(seriesA, seriesB):
  a = set(seriesA)
  b = set(seriesB)
  overlap = len(a.intersection(b))/len(a.union(b))
  return overlap

def makeSet(seriesA):
  return set(seriesA)

#%%
buyer_set = pd.DataFrame(buyers.groupby(["token_address","date"])["to_address"].apply(makeSet))
seller_set = pd.DataFrame(sellers.groupby(["token_address","date"])["from_address"].apply(makeSet))
# %%
buy_sell_set = buyer_set.merge(seller_set,how="outer", on=["token_address","date"])
#%%
buy_sell_set["date"] = pd.to_datetime(buy_sell_set["date"])
buy_sell_set = buy_sell_set.loc[buy_sell_set["date"]<"2021-03-01"]
buy_sell_set["intersect"] = [len(a.intersection(b)) for a,b in zip(buy_sell_set["to_address"],buy_sell_set["from_address"])]
buy_sell_set["union"] = [len(a.union(b)) for a,b in zip(buy_sell_set["to_address"],buy_sell_set["from_address"])]
buy_sell_set["buy_sell_overlap"] = buy_sell_set["intersect"]/buy_sell_set["union"]

buy_sell_set = buy_sell_set.reset_index()
buy_sell_set.to_csv("buyer_seller_overlap.csv", index=False)

# %%
#some "from_address" are empty?