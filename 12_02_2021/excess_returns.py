#%%
import requests,csv
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt 
from matplotlib.backends.backend_pdf import PdfPages
import sklearn 
import statsmodels
from datetime import datetime, date
import json
from scipy.stats import mstats
from scipy.stats import describe
import statsmodels.api as sm
import lxml

pd.options.display.float_format = '{:,.8f}'.format
# %% daily log difference for market data, some onchain data, and market index

# compute market return, daily and weekly, merge with onchain data
#daily market return
market_index = pd.read_csv("market_index.csv")
market_index["date"] = pd.to_datetime(market_index["date"])
market_index = market_index.sort_values("date")
market_index = market_index.loc[(market_index["date"]<"2021-03-04") & (market_index["date"]>"08-22-2016" )]
market_index["market_return"] = np.log(market_index["price_index"])-np.log(market_index["price_index"].shift(1))

market_index = market_index.set_index("date")
#weekly market return
index_w = market_index.resample('W').apply({"price_index":'last'})
index_w["market_return_wk"] = np.log(index_w["price_index"]).diff()
index_w = index_w.drop("price_index", axis=1)
market_index = market_index.merge(index_w, how="outer",on="date") #merge daily and weekly market return

df = pd.read_csv("onchain_stata2.csv")
#df = df.drop(columns = "Unnamed: 0")
df["date"] = pd.to_datetime(df["date"])
df = df.set_index("date")
df = df.merge(market_index,how="outer",on="date") #merge onchain level data with market return level and chg
df = df.reset_index()
df = df.sort_values(["cmc_url","date"])
df.to_csv("index_return.csv", index=False)
#%%
df=pd.read_csv("index_return.csv")

dailyvars = ['close', 'volume', 'market_cap', 'positive_address_count',
       'zero_address_count', 'new_zero_count', 'new_positive_count',
       'num_transaction', 'unique_address', 'new_address', 'bears',
       'bears_sell_amount', 'bear_share', 'bulls', 'bulls_buy_amount',
       'bull_share', 'bull_bear', 'bull-bear_shares']

weeklyvars = ['token_supply','top_1perc_count', '1perc_balance', '1perc_share', '1perc_balance7',
       '1perc_buy_sell', 'token_supply_lead', '1perc_chg', '1perc_buy_share']

df["close"].loc[df["close"]==0]=np.nan
df = df.sort_values(["cmc_url","date"])


def logdiff(df,dailyvars):
  data = df
  #daily log difference
  for var in dailyvars:
    print(var)
    data[var+"_chg"] = data.groupby(["cmc_url"])[var].apply(lambda x: (np.log(x)).diff())
  #weekly log difference
  week = data[["date","cmc_url"]+dailyvars]
  week["date"] = pd.to_datetime(week["date"])
  week = week.set_index("date")
  weekly = week.groupby("cmc_url").resample('W').last()
  weekly = weekly.drop("cmc_url",axis=1)
  weekly = weekly.groupby("cmc_url")[dailyvars].apply(lambda x: (np.log(x)).diff())
  weekly = weekly.reset_index()

  for var in dailyvars:
    weekly = weekly.rename(columns={var:var+"_chg_wk"})

  data["date"] = pd.to_datetime(data["date"])
  data = data.merge(weekly,how="outer",on=["cmc_url","date"])
  
  return data

df = logdiff(df,dailyvars)
df["extra_return"] = df["close_chg"] - df["market_return"]
df["extra_return_wk"] = df["close_chg_wk"] - df["market_return_wk"]


# %% count days and weeks since CMC availability

#add column of days since exchange listing
def list_days(df,vars,newvar): #newvar is the column of days since listing
  df = df[:]
  df['date']  = pd.to_datetime(df['date'])
  
  df.loc[df.groupby('cmc_url').apply(lambda x : x[vars[0]].dropna()).reset_index(level=1)['level_1'].min(level=0),newvar]=1
  df[newvar] = df.groupby('cmc_url')[newvar].fillna(method = "ffill")
  df[newvar] = df.groupby('cmc_url')[newvar].cumsum()
  
  return df

#add column of weeks since exchange listing
def list_weeks(df,vars,newvar): 
  df = df[:]
  data = df.groupby(['cmc_url', pd.Grouper(key='date',freq = "W")])[vars].last()
  data = data.reset_index() 

  data.loc[data.groupby('cmc_url').apply(lambda x : x[vars[0]].dropna()).reset_index(level=1)['level_1'].min(level=0),newvar]=1
  data[newvar] = data.groupby('cmc_url')[newvar].fillna(method = "ffill")
  data[newvar] = data.groupby('cmc_url')[newvar].cumsum()
  data = data.drop("close",axis=1)
  df = df.merge(data,how = "outer",on = ["cmc_url","date"])
  
  return df

df1=list_days(df,["close"],"days_listed") 
df2 = list_weeks(df1,["close"],"weeks_listed")
df2.to_csv("onchain_chg_stata2.csv",index=False)

#%%plot the weekly extra return for each week after launch

def launch_quantiles(df,var,count):
  data = df
  dat1 = pd.DataFrame(data.groupby(count)[var].quantile([0.25,0.5,0.75])).unstack(level=-1)["extra_return_wk"]
  dat2 = pd.DataFrame(data.groupby(count)[var].mean())
  dat3 = pd.DataFrame(data.groupby(count)[var].count())
  data = dat1.merge(dat2,on=count).rename(columns={"extra_return_wk":"mean"})
  data = data.merge(dat3,on=count).rename(columns={"extra_return_wk":"# tokens"})

  return data
  
extra_return_launch = launch_quantiles(df2,"extra_return_wk","weeks_listed").reset_index()

#plot two axis
def two_axis_plot(x,vars1, vars2,figname,title,xlabel,y1label,y2label):
  fig,ax1 = plt.subplots()
  ax2 = ax1.twinx()
  ax1.plot(x,vars1)
  ax2.plot(x,vars2,"b-")
  ax1.set_xlabel(xlabel)
  ax1.set_ylabel(y1label)
  ax1.legend(["0.25","0.5","0.75","mean"])
  #ax1.legend(loc=0)
  
  ax2.set_ylabel(y2label)
  ax2.legend("# tokens")
  plt.title(title)
  ax2.legend(loc=10)
  #fig.legend(loc="upper center", bbox_to_anchor=(1,1), bbox_transform=ax1.transAxes)
  fig.savefig(figname)
  plt.show()
#%%
x = extra_return_launch["weeks_listed"]
vars1 = extra_return_launch.loc[:,[0.25,0.5,0.75,"mean"]]
vars2 = extra_return_launch.loc[:,'# tokens']

two_axis_plot(x,vars1,vars2,"extra_return_llisting","token return - market return since listin","weeks since listing",
              "token return - market return","# tokens")

#%% Plot: divide tokens into quintiles based on cumulative return at 50 weeks. 

#for each token, compute daily cumulative percent chg for different variables, based on starting date of closing price

def cumu_chg(df, vars, freq):
  data = df[:]
  data = data.sort_values(["cmc_url","date"])
  vars_cumu = [var+"_cumu_chg" for var in vars]
  
  #for each token, compute daily cumulative percent chg for different variables, based on starting date of closing price
  #for var in vars:
  init = data[data["days_listed"]==1].index
  for var in vars:
    data['init_'+var] = np.nan
    data.loc[init,'init_'+var] = data[var][init]
    data['init_'+var] = data.groupby('cmc_url')['init_'+var].fillna(method = "ffill")

    #data['cumsum_'+var] = data.groupby('cmc_url')[var].cumsum()
    #data[var+'cumu_chg'] = (data['cumsum_'+var].divide(data['init_'+var])-1)*100
    data[var+'_cumu_chg'] = (data[var].divide(data['init_'+var])-1)*100

    data.drop(columns = ['init_'+var])
  
  #compute weekly cumulative returns
  vars_cumu_wk = [var+"_cumu_chg_wk" for var in vars]
  if freq=="wk":
    weekly = data.groupby(['cmc_url', pd.Grouper(key='date',freq = "W")])[vars_cumu].last()
    #rename variables to add "_wk"
    name_dict = dict(zip(vars_cumu,vars_cumu_wk))
    weekly = weekly.rename(columns=name_dict)
    #merge cumulative weekly return back to the original dataset
    data = data.merge(weekly, how="outer",on=["cmc_url","date"])

  return data

vars = ["close","price_index"]
df3 = cumu_chg(df2,vars,"wk").sort_values(["cmc_url","date"])
df3["excess_return_cumu_chg"] = df3["close_cumu_chg"] - df3["price_index_cumu_chg"]
df3["excess_return_cumu_chg_wk"] = df3["close_cumu_chg_wk"] - df3["price_index_cumu_chg_wk"]

'''
df3.loc[df3["cmc_url"]=="/currencies/augur/"][["date","days_listed","volume","close_chg","close_cumu_chg","close_cumu_chg_wk",'price_index_cumu_chg',
      "price_index","close","init_close"]].sort_values("date").head(20)
      '''
#%%
# compute quintiles of excess cumulative return after wk_num weeks, find list of tokens in each quintile
def percentiles(df,wk_num, vars):
  data = df[:]

  #find tokens in each quintile group
  dat1 = data.reset_index().set_index("weeks_listed")
  week = dat1[["cmc_url"] + [vars]].loc[wk_num] # all token's cumulative return in week wk_num
  cutoffs = week[vars].quantile([0.2,0.4,0.6,0.8])
  first  = week.loc[week[vars] < cutoffs.loc[0.2]]["cmc_url"].to_list()
  second = week.loc[(week[vars] >= cutoffs.loc[0.2]) & (week[vars] < cutoffs.loc[0.4])]["cmc_url"].to_list()
  third  = week.loc[(week[vars] >= cutoffs.loc[0.4]) & (week[vars] < cutoffs.loc[0.6]) ]["cmc_url"].to_list()
  fourth = week.loc[(week[vars] >= cutoffs.loc[0.6]) & (week[vars] < cutoffs.loc[0.8]) ]["cmc_url"].to_list()
  fifth  = week.loc[(week[vars] >= cutoffs.loc[0.8])]["cmc_url"].to_list()

  return first,second, third, fourth, fifth

wk_num = 52
vars = "excess_return_cumu_chg_wk"
a, b, c, d, e = percentiles(df3,wk_num,vars)

one = df3.loc[df3.cmc_url.isin(a) & (df3.weeks_listed <= wk_num)][["cmc_url","weeks_listed","date","market_cap",vars]]
two = df3.loc[df3.cmc_url.isin(b) & (df3.weeks_listed <= wk_num)][["cmc_url","weeks_listed","date","market_cap",vars]]
three = df3.loc[df3.cmc_url.isin(c) & (df3.weeks_listed <= wk_num)][["cmc_url","weeks_listed","date","market_cap",vars]]
four = df3.loc[df3.cmc_url.isin(d) & (df3.weeks_listed <= wk_num)][["cmc_url","weeks_listed","date","market_cap",vars]]
five = df3.loc[df3.cmc_url.isin(e) & (df3.weeks_listed <= wk_num)][["cmc_url","weeks_listed","date","market_cap",vars]]

#%% compute weighted averages

def weighted_average(df,var, weight):
  data = df[:]
  data = data.set_index(["cmc_url","weeks_listed"])
  data = data.unstack(level=0)
  data.columns = data.columns.rename(['variables',"cmc_url"])
  
  weights = data[weight]
  weights[weight+"_total"]= weights.sum(axis=1)
  weights = weights.div(weights[weight+"_total"], axis=0)
  weights = weights.drop(weight+"_total",axis=1)
  
  var1 = data[var]
  mult = var1*weights.values
  var1["index"] = mult.sum(axis=1)
  return var1

#weighted average for each quintile
index1 = weighted_average(one,"excess_return_cumu_chg_wk","market_cap").reset_index()[["weeks_listed","index"]]
index2 = weighted_average(two,"excess_return_cumu_chg_wk","market_cap").reset_index()[["weeks_listed","index"]]
index3 = weighted_average(three,"excess_return_cumu_chg_wk","market_cap").reset_index()[["weeks_listed","index"]]
index4 = weighted_average(four,"excess_return_cumu_chg_wk","market_cap").reset_index()[["weeks_listed","index"]]
index5 = weighted_average(five,"excess_return_cumu_chg_wk","market_cap").reset_index()[["weeks_listed","index"]]

#%% plotting function

def plot_series(x,yvars,leg,xlabel,ylabel,figname,title):
  fig = plt.figure()
  for i in range(len(yvars)):
    plt.plot(x,yvars[i])
  
  plt.xlabel(xlabel)
  plt.ylabel(ylabel)
  plt.title(title)
  plt.legend(leg)
  plt.show()
  fig.savefig(figname)

x = index1["weeks_listed"]
yvars =[index1["index"], index2["index"],index3["index"],index4["index"],index5["index"]]
xlabel = "weeks since listing"
ylabel = "percentage point"
title = "token price cumulative return - market cumulative return"
figname = "extra_return_cumu"
leg=["quintile 1","quintile 2","quintile 3","quintile 4","quintile 5"]

plot_series(x,yvars,leg,xlabel,ylabel,figname,title)
#%%

df_market.market_cap = df_market.market_cap.replace(np.nan,0)
df_market.volume = df_market.volume.replace(np.nan,0)
df_market = df_market.loc[df_market.volume>1000] #delete tokens-days with less than $1000 in trading volume

df_market = df_market.set_index(['cmc_url','weeks'])
df_market = df_market.unstack(level=0) 
df_market.index = pd.to_datetime(df_market.index)
df_market = df_market.loc[df_market.index<="2021-03-03"]
df_market.market_cap = df_market.market_cap.replace(np.nan,0)
df_market.volume = df_market.volume.replace(np.nan,0)
df_market.columns = df_market.columns.rename(['variables',"cmc_url"])
tokens = df_market["market_cap"].columns.tolist()

df_mc = df_market.market_cap
df_mc["total_MC"] = df_mc.sum(axis=1)
df_mc = df_mc.div(df_mc.total_MC,axis=0)
df_mc = df_mc.drop("total_MC",axis=1)

df_price = df_market.close
mult = df_price*df_mc.values
df_price["price_index"] = mult.sum(axis=1)
df_price["count"] = df_price.count(axis=1)

#%%
vars = ["close_chg","market_return"]
df3 = cumu_chg(df2,vars,"wk").sort_values(["cmc_url","date"])

#%%fill in rows of missing dummy variables. 
cols = A.columns.tolist()
start = cols.index("github")
for i in range(start,len(cols)-1):
    A[cols[i]]= A.groupby("cmc_url")[cols[i]].ffill()
    A[cols[i]]= A.groupby("cmc_url")[cols[i]].bfill()

#fill in "exchange_weeks" for every day of the week
df2 = A
df2["exchange_weeks"] = df2.groupby("cmc_url")["exchange_weeks"].ffill()
# %%
df2 = df2.set_index(['cmc_url','date'])
df2=df2.unstack(level=0) 