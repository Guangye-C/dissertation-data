#%% Collects onchain data from Bigquery 
# Output files:
  # 1. cmc_contract_all.csv
      # all 2000 tokens, with columns for cmc_url, and contract addresses for Ethereum, BSC, Tron, Solana, Algorand, Heco, Xdai, and Polygon
      # from cmc_contract1.csv + cmc_contract.csv
  # 2. BQ_token_address.txt
      # One string of all ERC20 token contract addresses, in the form "token_address = A OR token_address = B"
      # from cmc_contract_all.csv 
  # 3. bq_transaction_data.csv
      # for 1152 erc20 tokens: daily number of transactions, unique addresses involved in transaction, unique new addresses 
  # 4. bq_address_balance_data.csv
      # daily # of addresses with positive balance, # of addresses with 0 balance, 
      #  # of addresses that became 0 balance, daily # of address that went from 0 to positive balance. 
  # 5. bq_bulls_bears.csv
      # each day, count the number of addresses that purchased more than' 1% of total traded volume (bulls)
      # each day, count the number of addresses that SOLD more than 1% of total traded volume (bears)
      # each day, the total amount (and shares) purhcased by bulls
      # each day, the total amount (and shares) sold by bears
  # 6. bq_balance_share.csv
      # calculate each address's share of total balance, rank by share
      # for largest addresses that each hold more than 1% of total balance, are they net buyer or seller the next day?
      # also include daily total supply of each token, daily share of total supply of each address
      # sum of balance by addresses who own more than 1%, as percentage of total supply. 
  # 7. 1perc_balance_sunday.csv
      # For each sunday for each token: 
          #   token_supply: token supply, 
          #   top_1perc_count: # of addresses owning more than 1% of token supply (largest owners),
          #   1perc_balance: Sum of the balance of largest owners, 
          #   1perc_share: percent of of token supply owned by the largest owners,
          #   1perc_balance7: total balance of largest holders 7 days later (following Sunday), 
          #   1perc_buy_sell: number of largest holders who were net buyers  -  number of largest holders who were net sellers
          #   token_supply _lead: token supply following week
          #   1perc_chg: largest holders' net buy / sale as percentage of token supply end of week.
          #   1perc_buy_share: % of largest holders that were net buyer / seller following week
    # 8. bulls_bears_trade.csv
        # same data as bq_bulls_bears.csv plus 2 more variables:
            #  bull/bear: num of bulls divided by num of bears
            #  bull-bear_share: bulls' % of daily trade - bears' % of daily trade

import pandas as pd
import numpy as np
import datetime
from google.cloud import bigquery
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/home/guangye/token_empirics/data_collection/March_2021/onchain_data/data-descriptive-e9f0ec54c5b2.json"
#%% get list of erc20 contract data
cmc_contracts1 = pd.read_csv("/home/guangye/token_empirics/data_collection/March_2021/market_data/cmc_contract1.csv")
cmc_contracts = pd.read_csv("/home/guangye/token_empirics/data_collection/March_2021/market_data/cmc_contract.csv")

cmc_contracts1.iloc[0:103] = cmc_contracts.iloc[0:103]
cmc_contract_all = cmc_contracts1

cmc_contract_all.to_csv("cmc_contract_all.csv", index=False)
#%%
cmc_contract_all = pd.read_csv("cmc_contract_all.csv", index_col=0)
contracts = cmc_contract_all['Ethereum'].dropna().str.lower() # contracts on BQ are all lower case

allContract = ""

for contract in contracts:
    allContract= f"{allContract} token_address = '{contract}' OR"   

allContract = allContract[:len(allContract)-3]
#print(allContract)

file = open('BQ_token_address.txt',"w")
file.write(allContract)
file.close()
#%% 
file = open('BQ_token_address.txt',"r")
allContract = file.read()
#%% Perform a query.   daily number of transactions, unique addresses involved in transaction, unique new addresses 

client = bigquery.Client()

QUERY = (
    '''
  WITH
  addresses AS (
  SELECT
    EXTRACT(YEAR
    FROM
      block_timestamp) AS year,
    EXTRACT(month
    FROM
      block_timestamp) AS month,
    EXTRACT(day
    FROM
      block_timestamp) AS day,
    FORMAT_TIMESTAMP("%Y-%m-%d",  block_timestamp) AS formatted_timestamp,
    from_address AS address,
    transaction_hash,
    token_address,
    block_timestamp
  FROM
    `bigquery-public-data.crypto_ethereum.token_transfers`
  WHERE '''+ allContract + '''
    
    UNION ALL
  SELECT
    EXTRACT(YEAR
    FROM
      block_timestamp) AS year,
    EXTRACT(month
    FROM
      block_timestamp) AS month,
    EXTRACT(day
    FROM
      block_timestamp) AS day,
    FORMAT_TIMESTAMP("%Y-%m-%d",  block_timestamp) AS formatted_timestamp,
    to_address AS address,
    transaction_hash,
    token_address,
    block_timestamp
  FROM
    `bigquery-public-data.crypto_ethereum.token_transfers`
    WHERE '''+ allContract + '''
    )

    SELECT
    COUNT(DISTINCT(A.transaction_hash)) AS num_transaction,
    COUNT(DISTINCT(A.address)) AS unique_address,
    A.token_address,
    A.formatted_timestamp,
    min(B.new_addresses) as new_address
    
    FROM
    addresses A
    
    left join
    (
    SELECT
        COUNT(*) AS new_addresses, formatted_timestamp, token_address
    FROM ( 
        SELECT
        token_address, FORMAT_DATE("%Y-%m-%d",MIN((CAST(block_timestamp AS date)))) AS formatted_timestamp
        FROM
        addresses
        GROUP BY
        token_address,
        address )
    Group by token_address, formatted_timestamp
    ) B
    on A.token_address = B.token_address AND A.formatted_timestamp= B.formatted_timestamp
    GROUP BY
    A.token_address,
    A.formatted_timestamp

    Order by 
    A.token_address,
    A.formatted_timestamp
        '''
)

query_job = client.query(QUERY)  # API request
rows = query_job.result()  # Waits for query to finish
print('finished query')

df = client.query(QUERY).to_dataframe(create_bqstorage_client=True)
df.to_csv("bq_transaction_data.csv",index=False)

print('finished saving bq_transaction_data.csv')
#%%
 # %%#per token: daily # of addresses that became 0 balance, daily # of address that went from 0 to positive balance. 

client = bigquery.Client()

QUERY1 = (
    '''
  with double_entry_book as (
  SELECT
    date(block_timestamp) AS date,
    from_address AS address,
    transaction_hash,
    token_address,
    block_timestamp,
    -CAST(value AS float64 ) AS daily_transfer
  FROM
    `bigquery-public-data.crypto_ethereum.token_transfers`
  WHERE '''+ allContract + '''
    
  UNION ALL
  SELECT
    date(block_timestamp) AS date,
    to_address AS address,
    transaction_hash,
    token_address,
    block_timestamp,
    CAST(value AS float64 ) AS daily_transfer
  FROM
    `bigquery-public-data.crypto_ethereum.token_transfers`
  WHERE '''+ allContract + '''
),
double_entry_book_grouped_by_date as (
    select token_address, address, sum(daily_transfer) as balance_increment, date
    from double_entry_book
    group by token_address,address, date
),
daily_balances_with_gaps as (
    select token_address, address, date, sum(balance_increment) over (partition by address order by date) as balance,
    lead(date, 1, current_date()) over (partition by address order by date) as next_date
    from double_entry_book_grouped_by_date
 
),
calendar AS (
    select date from unnest(generate_date_array('2015-07-30', current_date())) as date
),

daily_balances as (
    select token_address, address, calendar.date, balance
    from daily_balances_with_gaps
    join calendar on daily_balances_with_gaps.date <= calendar.date AND calendar.date < daily_balances_with_gaps.next_date
),

daily_balances_lag as (
    select token_address, address, date, balance,
    LAG(balance) OVER (PARTITION BY address ORDER BY date ASC) AS lag_balance
    from daily_balances
)


select * from(
select date, count(*) as positive_address_count, token_address
from daily_balances_lag
where balance > 0
group by token_address, date) as positive

left join (
  select date, count(*) as zero_address_count, token_address
  from daily_balances_lag
  where balance = 0
  group by token_address,date) as zero

on positive.date = zero.date and positive.token_address = zero.token_address

left join (
  select date, count(*) as new_zero_count, token_address
  from daily_balances_lag
  where balance = 0 and (lag_balance!=0 or lag_balance is null)
  group by token_address, date) as new_zero
 on zero.date = new_zero.date and new_zero.token_address = zero.token_address

left join (
  select date, count(*) as new_positive_count, token_address
  from daily_balances_lag
  where balance > 0 and lag_balance=0
  group by token_address, date) as new_positive
 on new_positive.date = new_zero.date and new_zero.token_address = new_positive.token_address

order by positive.token_address, positive.date
    '''
)
query_job = client.query(QUERY1)  # API request
rows = query_job.result()  # Waits for query to finish
print('finished query')

bq_address = client.query(QUERY1).to_dataframe(create_bqstorage_client=True)
bq_address.to_csv("bq_address_balance_data.csv",index=False)
print('finished saving bq_address_balance_data.csv')
#%% Process bq_address_data,csv
address_balance = pd.read_csv("bq_address_balance_data.csv", index_col=0)
address_balance = address_balance.reset_index()
address_balance["date"] = pd.to_datetime(address_balance["date"])
address_balance = address_balance.drop(['date_1','token_address_1','date_2',"token_address_2","date_3","token_address_3"],axis=1)

address_balance.to_csv("address_count.csv", index=None)
# %% bulls and bears:
## each day, count the number of addresses that purchased more than 1% of total traded volume (bulls)
## each day, count the number of addresses that SOLD more than 1% of total traded volume (bears)
## each day, the total amount purhcased by bulls
## each day, the total amount sold by bears
'''
cmc_contract_all = pd.read_csv("cmc_contract_all.csv")
contracts = cmc_contract_all['Ethereum'].dropna().str.lower() # contracts on BQ are all lower case

allContract = ""

for contract in contracts:
    allContract= f"{allContract} token_transfers.token_address = '{contract}' OR"   


allContract = allContract[:len(allContract)-3]
#print(allContract)

file = open('BQ_tokentransfer_token_address.txt',"w")
file.write(allContract)
file.close()
'''

file = open('BQ_token_address.txt',"r")
allContract = file.read()

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
    select to_address as to_address, sum(CAST(value AS float64)) as purchase_value, date(block_timestamp) as date, token_address
    from `bigquery-public-data.crypto_ethereum.token_transfers`
    where ''' +allContract+ '''
    group by token_address, to_address, date
)

,double_entry_sell as (
    -- credits
    select from_address as from_address, sum(CAST(value AS float64)) as sold_value, date(block_timestamp) as date, token_address
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
    select safe_divide(purchase_value, trade_total) as purchase_share, transaction_sum.date, transaction_sum.token_address as token_address, to_address, purchase_value,
    row_number() over (partition by transaction_sum.token_address, double_entry_buy.date order by purchase_value desc) as buy_rank
    from double_entry_buy 
    join transaction_sum on double_entry_buy.date = transaction_sum.date and double_entry_buy.token_address = transaction_sum.token_address
    where safe_divide(purchase_value, trade_total) >= 0.01
)
,sell_shares as(
    select safe_divide(sold_value, trade_total) as sold_share, transaction_sum.date, transaction_sum.token_address as token_address, from_address, sold_value,
    row_number() over (partition by transaction_sum.token_address, double_entry_sell.date order by sold_value desc) as sell_rank
    from double_entry_sell
    join transaction_sum on double_entry_sell.date = transaction_sum.date and double_entry_sell.token_address = transaction_sum.token_address
    where safe_divide(sold_value, trade_total) >= 0.01
)


,seller_count as (
     select count(from_address) as whale_sellers,  sum(sold_value) as whale_sell_amount, token_address, date, sum(sold_share) as bear_share
    from sell_shares 
    group by token_address,date
)

,buyer_count as (
     select count(to_address) as whale_buyers,  sum(purchase_value) as whale_buy_amount, token_address, date, sum(purchase_share) as bull_share
    from buy_shares 
    group by token_address,date
)

select *
from seller_count 
full outer join buyer_count on buyer_count.token_address =seller_count.token_address and buyer_count.date = seller_count.date
order by seller_count.token_address, seller_count.date
'''
)
query_job = client.query(QUERY2)  # API request
rows = query_job.result()  # Waits for query to finish
print('finished query')

bq_bulls_bears = client.query(QUERY2).to_dataframe(create_bqstorage_client=True)

bq_bulls_bears.to_csv("bq_bulls_bears.csv",index=False)

print('finished saving bq_bulls_bears.csv')
#%% Process bq_bulls_bears.csv
bulls_bears = pd.read_csv("bq_bulls_bears.csv")
bulls_bears.loc[bulls_bears["token_address"].isnull(), 'token_address'] = bulls_bears['token_address_1']
bulls_bears.loc[bulls_bears["date"].isnull(), 'date'] = bulls_bears['date_1']

bulls_bears = bulls_bears.drop(['token_address_1','date_1'],axis=1)
bulls_bears = bulls_bears.rename(columns={"whale_sellers":"bears","whale_sell_amount":"bears_sell_amount","whale_buyers":"bulls","whale_buy_amount":"bulls_buy_amount"})
bulls_bears["bull/bear"] = bulls_bears["bulls"]/bulls_bears["bears"]
bulls_bears["bull-bear_shares"] = bulls_bears["bull_share"] - bulls_bears["bear_share"]

bulls_bears.to_csv("bulls_bears_trade.csv", index=None)

# %%#calculate each address's share of total balance, rank by share
# for largest addresses that each hold more than 1% of total balance, are they net buyer or seller the next day?

file = open('BQ_token_address.txt',"r")
allContract = file.read()

client = bigquery.Client()

QUERY3 = (
'''
with 
double_entry_book as (
    -- debits
    select to_address as address, CAST(value AS float64) as value, block_timestamp,token_address
    from `bigquery-public-data.crypto_ethereum.token_transfers`
    where from_address is not null and to_address is not null
    and (''' +allContract+ ''')
    union all
    -- credits
    select from_address as address, -CAST(value AS float64) as value, block_timestamp,token_address
    from `bigquery-public-data.crypto_ethereum.token_transfers`
    where from_address is not null and to_address is not null
    and (''' +allContract+ ''')
)

,token_decimals as (
    select address, cast(decimals as int64) as decimals
    from `bigquery-public-data.crypto_ethereum.tokens`
)

,double_entry_book_by_date as (
    select 
        date(block_timestamp) as date, 
        address, 
        sum(value) as value,
        token_address
    from double_entry_book
    group by token_address,address, date
)

,daily_balances_with_gaps as (
    select 
        address, 
        date,
        token_address,
        sum(value) over (partition by token_address, address order by date) as balance,
        lead(date, 1, current_date()) over (partition by token_address, address order by date) as next_date
        from double_entry_book_by_date        
)

,calendar as (
    select date from unnest(generate_date_array('2015-07-30', current_date())) as date
)

,daily_balances as (
    select address, calendar.date, balance, token_address
    from daily_balances_with_gaps
    join calendar on daily_balances_with_gaps.date <= calendar.date and calendar.date < daily_balances_with_gaps.next_date
    where balance >= 0
)

,supply as (
    select
        date,
        token_address,
        sum(balance) as daily_supply
    from daily_balances
    group by token_address, date
)

,daily_balances_share as (
    select 
        daily_balances.date,
        balance,
        address,
        daily_supply,
        daily_balances.token_address as token_address,
        row_number() over (partition by daily_balances.token_address, daily_balances.date order by balance desc) as rank,
        safe_divide(balance,daily_supply) as balance_share
    from daily_balances
    join supply on daily_balances.date = supply.date and daily_balances.token_address = supply.token_address
    where safe_divide(balance, daily_supply) >= 0.0001
)

,daily_balance_lead as (
    select
        date,
        balance,
        address,
        token_address,
        daily_supply,
        rank,
        balance_share,
        sum(balance_share) over (partition by token_address, date order by rank) as cumu_share,
        Lead(balance, 1 ) OVER (PARTITION BY token_address, address ORDER BY date ASC) AS lag_balance1,
        Lead(balance, 2 ) OVER (PARTITION BY token_address, address ORDER BY date ASC) AS lag_balance2,
        Lead(balance, 3 ) OVER (PARTITION BY token_address, address ORDER BY date ASC) AS lag_balance3,
        Lead(balance, 4 ) OVER (PARTITION BY token_address, address ORDER BY date ASC) AS lag_balance4,
        Lead(balance, 5 ) OVER (PARTITION BY token_address, address ORDER BY date ASC) AS lag_balance5,
        Lead(balance, 6 ) OVER (PARTITION BY token_address, address ORDER BY date ASC) AS lag_balance6,
        Lead(balance, 7 ) OVER (PARTITION BY token_address, address ORDER BY date ASC) AS lag_balance7
    from daily_balances_share
)

select *
from daily_balance_lead
where balance_share >=0.01
order by token_address,date, rank
'''
)

query_job = client.query(QUERY3)  # API request
rows = query_job.result()  # Waits for query to finish
print('finished query')

bq_balance_share = client.query(QUERY3).to_dataframe(create_bqstorage_client=True)

bq_balance_share.to_csv("bq_balance_share.csv",index=False)

print('finished saving bq_balance_share.csv')
# %%Process bq_balance_share
# keep only sundays
# For each sunday for each token: 
        #   token_supply: token supply, 
        #   top_1perc_count: # of addresses owning more than 1% of token supply (largest owners),
        #   1perc_balance: Sum of the balance of largest owners, 
        #   1perc_share: percent of of token supply owned by the largest owners,
        #   1perc_balance7: total balance of largest holders 7 days later (following Sunday), 
        #   1perc_buy_sell: number of largest holders who were net buyers  -  number of largest holders who were net sellers
        #   token_supply _lead: token supply one week ahead
        #   1perc_chg: largest holders' net buy / sale as percentage of token supply end of week.
        #   1perc_buy_share: % of largest holders that were net buyer / seller following week

bq_balance_share = pd.read_csv("bq_balance_share.csv")
#reorder columns
cols = list(bq_balance_share)
cols = ['date','token_address','daily_supply','address','rank','balance','balance_share',
 'cumu_share','lag_balance1','lag_balance2','lag_balance3','lag_balance4','lag_balance5',
 'lag_balance6','lag_balance7']

bq_balance_share = bq_balance_share.loc[:,cols]
bq_balance_share["date"] = pd.to_datetime(bq_balance_share['date'], format='%Y-%m-%d')
bq_balance_share['day'] = bq_balance_share["date"].dt.dayofweek
balance_weekly = bq_balance_share[bq_balance_share["day"]==6] # Sunday is 6
weekly_cols = ['date','day','token_address','daily_supply','address','rank','balance','balance_share',
                'cumu_share','lag_balance7']
balance_weekly = balance_weekly.loc[:,weekly_cols]
balance_weekly["weekly_chg"] = np.where(balance_weekly["lag_balance7"] - balance_weekly["balance"]>0,1,np.where(balance_weekly["lag_balance7"] - balance_weekly["balance"]==0,0,-1))

balance_sunday = balance_weekly.groupby(['token_address','date']).agg({"daily_supply":"mean",
                                                      "rank":"max",
                                                      "balance":"sum",
                                                      "cumu_share": "max",
                                                      "lag_balance7": "sum",
                                                      "weekly_chg": "sum"})

balance_sunday = balance_sunday.rename(columns={"daily_supply":"token_supply", "rank":"top_1perc_count","balance":"1perc_balance","cumu_share":"1perc_share","lag_balance7":"1perc_balance7","weekly_chg":"1perc_buy_sell"})
balance_sunday = balance_sunday.reset_index()
balance_sunday['token_supply_lead'] = balance_sunday.groupby(['token_address'])['token_supply'].shift(-1)
#calculate largest holders' net buy / sale as percentage of token supply end of week. 
#ie: the largest holders bought/sold x% of token supply this week. 
balance_sunday['1perc_chg'] = (balance_sunday['1perc_balance7'] - balance_sunday['1perc_balance'])/balance_sunday['token_supply_lead']
#calculate % of largest holders that were net buyer / seller following week
balance_sunday['1perc_buy_share'] = balance_sunday['1perc_buy_sell'] /balance_sunday['top_1perc_count']

balance_sunday.to_csv("1perc_balance_sunday.csv")
# %%  Merge all onchain data together

top1_balance = pd.read_csv("1perc_balance_sunday.csv", index_col=0)
top1_balance["date"] = pd.to_datetime(top1_balance["date"])
transactions = pd.read_csv("bq_transaction_data.csv")
transactions["date"] = pd.to_datetime(transactions["formatted_timestamp"])
transactions = transactions.drop(['formatted_timestamp'],axis=1)
bulls_bears = pd.read_csv("bq_bulls_bears.csv")
bulls_bears['date'] = pd.to_datetime(bulls_bears['date'])
addresses = pd.read_csv("address_count.csv")
addresses['date'] = pd.to_datetime(addresses['date'])

merge1 = addresses.merge(transactions,how="outer",on=['token_address', "date"])
merge2 = merge1.merge(bulls_bears,how="outer",on=['token_address', "date"])
onchain_data = merge2.merge(top1_balance, how="outer",on=['token_address', "date"])
cols = ['date', 'token_address','positive_address_count',  'zero_address_count',
       'new_zero_count', 'new_positive_count', 'num_transaction',
       'unique_address', 'new_address', 'bears', 'bears_sell_amount',
       'bear_share', 'bulls', 'bulls_buy_amount', 'bull_share', 'bull/bear',
       'bull-bear_shares', 'token_supply', 'top_1perc_count', '1perc_balance',
       '1perc_share', '1perc_balance7', '1perc_buy_sell', 'token_supply_lead',
       '1perc_chg', '1perc_buy_share']
onchain_data = onchain_data.loc[:,cols]

onchain_data = onchain_data.sort_values(by=['token_address',"date"])
onchain_data.to_csv("onchain_data.csv",index=None)
# %%
