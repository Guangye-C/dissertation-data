
#Make plots by quintiles. Two types of quintiles:
  #1.  each day, divide observations into quintiles based on a certain variable, say cumulative above market return.
        # then plot the weighted and simple averages of another variable for each quintile. Weighted by market cap
  #2. pick a day/week after launching, then divide into quintiles based on a variable on that day
        # then plot the weighted and simple averages of another variable for each quintile, x-variable being days/weeks since launching
  #3. for each day / week after launching, divide observations into quintiles based on a certain variable, say cumulative above market return
        # # then plot the weighted and simple averages of another variable for each quintile. Weighted by market cap
        # x-axis is days / weeks since launching
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

pd.options.display.float_format = '{:,.6f}'.format

df = pd.read_csv("onchain_chg_stata2.csv")
#df = df.drop("Unnamed: 0", axis=1)
df["date"] = pd.to_datetime(df["date"])
df1 = df[:]

#%% compute quintiles of market cap, find list of tokens in each quintile for each day
def qcut(s, q):
    labels = ['q{}'.format(i) for i in range(1, q+1)]
    return pd.qcut(s, q, labels=labels,duplicates="drop")

#each day, divide into bins based on var value

def bins(df,bin,var):
  data = df[:]
  counts = data.groupby("date")[var].nunique()
  # create a dummy =1 if there are more tokens than number of bins on that day
  # ie: for quintiles, need at least 5 tokens everyday. For deciles, need 10
  small = counts[counts<bin].index.to_list()
  data["more"+str(bin)] = np.nan
  data.loc[~data.date.isin(small),"more"+str(bin)]=1
  data["date"] = pd.to_datetime(data["date"])
  data = data.set_index(["date","cmc_url"])
  cut = (data.loc[data["more"+str(bin)].notna()]).groupby("date")[var].apply(qcut,q=bin)
  print(cut)
  cut = cut.to_frame().rename(columns={var:var+str(bin)})
  data = data.drop("more"+str(bin),axis=1)
  dat1 = data.merge(cut,how="outer",on = ["date","cmc_url"])
  
  return dat1

#%%
df2 = bins(df1,5,"market_cap").reset_index()
df3 = bins(df2,10,"market_cap").reset_index()
df4 = bins(df3,5,"days_listed").reset_index()

#%%
df4.to_csv("onchain_chg_bins.csv")

#%% plot simple average whale balance share by market cap

#break dataset into separate datasets, one for each quintile of var.
#results in dictionary where the key "one" corresponds to dataset (dataframe) of the first quintile
def separate_bins(df, var,bins):
  data = df[:]
  x = ["one","two","three","four","five","six","seven","eight","nine","ten"]
  k = len(bins)
  
  quints = []
  for i in range(1,len(bins)+1):
    print(i)
    quints.append(df4.loc[df4[var]=="q"+str(i)])

  A = dict(zip(x[:k],quints))
  
  return A

quints = separate_bins(df4,"market_cap5",["q1", "q2","q3","q4","q5"])
#%% 
def plot_series(x,yvars,leg,xlabel,ylabel,figname,title):
  fig = plt.figure()
  for i in range(len(yvars)):
    plt.plot(x,yvars[i])
  
  plt.xlabel(xlabel)
  plt.ylabel(ylabel)
  plt.title(title)
  plt.legend(leg)
  
  #fig.savefig(figname)
  plt.show()
  
#%% Simple average: for variable "var"

def simple_average(df,var, groupvar,bins):
  data = df
  keys = list(data)
  date = data[keys[0]]["date"]
  
  x = ["one","two","three","four","five","six","seven","eight","nine","ten"]
  k = len(bins)
  y = ["quintile1","quintile2","quintile3","quintile4","quintile5","quintile6","quintile7","quintile8","quintile9","quintile10"]
  
  quints = []
  for i in range(len(bins)):
    print(i)
    #for each quintile, each day, take the simple mean of varaible var, and rename it "quintile#"
    temp = pd.DataFrame(data[x[i]].loc[data[x[i]][var].notna()].groupby(groupvar)[var].mean()).rename(columns={var:y[i]}).reset_index()
    #quints.append(temp)

    if i == 0:
      Y = temp
    else: 
      Y = Y.merge(temp, how="outer",on=groupvar)
  return Y  

#%% weigted average
#      data is already sorted into quintiles based on either each day's value or a specific day's value

def weighted_average(df,var, groupvar, weight,bins):
  data = df
  keys = list(data)
  date = data[keys[0]]["date"]

  x = ["one","two","three","four","five","six","seven","eight","nine","ten"]
  k = len(bins)
  y = ["quintile1","quintile2","quintile3","quintile4","quintile5","quintile6","quintile7","quintile8","quintile9","quintile10"]
  
  #output = Dataframe
  for i in range(len(bins)):
    print(i)
    
    temp = data[x[i]].set_index(["cmc_url",groupvar])
    temp = temp.unstack(level=0)
    temp.columns = temp.columns.rename(['variables',"cmc_url"])
    weights = temp[weight]
    weights[weight+"_total"]= weights.sum(axis=1)
    weights = weights.div(weights[weight+"_total"], axis=0)
    weights = weights.drop(weight+"_total",axis=1)
    
    var1 = temp[var]
    mult = var1*weights.values
    var1[y[i]+"_weighted"] = mult.sum(axis=1)
    var1 = var1.reset_index()
    
    A = var1[[groupvar,y[i]+"_weighted"]]
  
    if i == 0:
      quints=A
    else:
      quints = quints.merge(A, how="outer",on=groupvar)
    
  return quints

# %%
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
df5 = cumu_chg(df4,vars,"wk").sort_values(["cmc_url","date"])
df5["excess_return_cumu_chg"] = df5["close_cumu_chg"] - df5["price_index_cumu_chg"]
df5["excess_return_cumu_chg_wk"] = df5["close_cumu_chg_wk"] - df5["price_index_cumu_chg_wk"]
df5["whale_each_share"] = df5["1perc_share"]/df5["top_1perc_count"]     
df5["bull_each_share"] = df5["bull_share"]/df5["bulls"]          
df5["bear_each_share"] = df5["bear_share"]/df5["bears"]                             
# %% Divide data into quintiles  based on a variable on a certain date/week
def percentiles(df,wk_num, vars):
  data = df[:]

  #find tokens in each quintile group
  dat1 = data.reset_index().set_index("weeks_listed")
  week = dat1[["cmc_url"] + [vars]].loc[wk_num] # all token's cumulative return in week wk_num
  cutoffs = week[vars].quantile([0.2,0.4,0.6,0.8])
  first  = week.loc[week[vars] < cutoffs.loc[0.2]]["cmc_url"].to_list() #extract list of tokens in each quintile
  second = week.loc[(week[vars] >= cutoffs.loc[0.2]) & (week[vars] < cutoffs.loc[0.4])]["cmc_url"].to_list()
  third  = week.loc[(week[vars] >= cutoffs.loc[0.4]) & (week[vars] < cutoffs.loc[0.6]) ]["cmc_url"].to_list()
  fourth = week.loc[(week[vars] >= cutoffs.loc[0.6]) & (week[vars] < cutoffs.loc[0.8]) ]["cmc_url"].to_list()
  fifth  = week.loc[(week[vars] >= cutoffs.loc[0.8])]["cmc_url"].to_list()

  return first,second, third, fourth, fifth


#%% 
groupvar = "date"
ave = simple_average(quints,"1perc_share",groupvar,["q1", "q2","q3","q4","q5"])
ave = ave.sort_values(groupvar)

x = ave[groupvar]
yvars =[ave["quintile1"], ave["quintile2"], ave["quintile3"], ave["quintile4"], ave["quintile5"]]
xlabel = "date"
ylabel = "percent"
title = "Simple average whale ownership share by market cap quintiles"
figname = "whale_ownership"
leg=["quintile 1","quintile 2","quintile 3","quintile 4","quintile 5"]

plot_series(x,yvars,leg,xlabel,ylabel,figname,title)

#%%
#weighted_ave = weighted_average(quints,"bear_share","date","market_cap",["q1", "q2","q3","q4","q5"])

vars = ["weeks_listed","cmc_url", "date","volume","market_cap",'positive_address_count',
 'zero_address_count',
 'new_zero_count',
 'new_positive_count',
 'num_transaction',
 'unique_address',
 'new_address',
 'bears',
 'bears_sell_amount',
 'bear_share',
 'bulls',
 'bulls_buy_amount',
 'bull_share',
 'bull_bear',
 'bull-bear_shares',
 'token_supply',
 'top_1perc_count',
 '1perc_balance',
 '1perc_share',
 '1perc_balance7',
 '1perc_buy_sell',
 'token_supply_lead',
 '1perc_chg',
 '1perc_buy_share',
 "whale_each_share",
 "excess_return_cumu_chg_wk"]


# %% weighted average for each quintile, quintiles determined daily
groupvar = "date"
weights = "market_cap"

for i in vars[3:]:
  average_w = weighted_average(quints,i,groupvar, weights,["q1","q2","q3","q4","q5"])

  x = average_w[groupvar]
  yvars =[average_w["quintile1_weighted"], average_w["quintile2_weighted"], average_w["quintile3_weighted"], average_w["quintile4_weighted"], average_w["quintile5_weighted"]]
  xlabel = groupvar
  #ylabel = "percent"
  title = "weighted average "+i+ " by "+ "market cap"+ " quintiles"
  figname = "weighted average"+i+ "by"+ "market cap"+ "quintiles"
  leg=["quintile 1","quintile 2","quintile 3","quintile 4","quintile 5"]

  plot_series(x,yvars,leg,xlabel,ylabel,figname,title)


# %% plot simple average of each quintile (based week 52 of market_cap or excess_return_cumu_chg_wk)
wk_num = 52
variable = "market_cap"   #"excess_return_cumu_chg_wk"
a, b, c, d, e = percentiles(df5,wk_num,variable)
more_var = ["cmc_url","weeks_listed","date"]

one = df5.loc[df5.cmc_url.isin(a) & (df5.weeks_listed <= wk_num)][vars]
two = df5.loc[df5.cmc_url.isin(b) & (df5.weeks_listed <= wk_num)][vars]
three = df5.loc[df5.cmc_url.isin(c) & (df5.weeks_listed <= wk_num)][vars]
four = df5.loc[df5.cmc_url.isin(d) & (df5.weeks_listed <= wk_num)][vars]
five = df5.loc[df5.cmc_url.isin(e) & (df5.weeks_listed <= wk_num)][vars]

quintiles = dict(zip(["one","two","three","four","five"],[one, two, three, four, five]))

#simple average plots
for i in vars[3:]:
  y1 = pd.DataFrame(one.loc[one[i].notna()].groupby("weeks_listed")[i].mean()).rename(columns={i:"quintile 1"}).reset_index()
  y2 = pd.DataFrame(two.loc[two[i].notna()].groupby("weeks_listed")[i].mean()).rename(columns={i:"quintile 2"}).reset_index()
  y3 = pd.DataFrame(three.loc[three[i].notna()].groupby("weeks_listed")[i].mean()).rename(columns={i:"quintile 3"}).reset_index()
  y4 = pd.DataFrame(four.loc[four[i].notna()].groupby("weeks_listed")[i].mean()).rename(columns={i:"quintile 4"}).reset_index()
  y5 = pd.DataFrame(five.loc[five[i].notna()].groupby("weeks_listed")[i].mean()).rename(columns={i:"quintile 5"}).reset_index()
  Y = y1.merge(y2, how = "outer", on="weeks_listed")
  Y= Y.merge(y3, how = "outer", on="weeks_listed")
  Y= Y.merge(y4, how = "outer", on="weeks_listed")
  Y= Y.merge(y5, how = "outer", on="weeks_listed").sort_values("weeks_listed").set_index("weeks_listed")

  fig,axs = plt.subplots()
  Y.plot(title = i+" simple average", ax=axs)
  plt.show()
  #fig.savefig(i+"_simple average")

# %% compute weighted average of each quintile (quintiles are based on week 52 of market_cap or another variable)

wk_num = 52
variable = "market_cap"   #"excess_return_cumu_chg_wk"
a, b, c, d, e = percentiles(df5,wk_num,variable)
more_var = ["cmc_url","weeks_listed","date"]

# a dataframe for each quintile
one = df5.loc[df5.cmc_url.isin(a) & (df5.weeks_listed <= wk_num)][vars]
two = df5.loc[df5.cmc_url.isin(b) & (df5.weeks_listed <= wk_num)][vars]
three = df5.loc[df5.cmc_url.isin(c) & (df5.weeks_listed <= wk_num)][vars]
four = df5.loc[df5.cmc_url.isin(d) & (df5.weeks_listed <= wk_num)][vars]
five = df5.loc[df5.cmc_url.isin(e) & (df5.weeks_listed <= wk_num)][vars]

quintiles = dict(zip(["one","two","three","four","five"],[one, two, three, four, five]))

groupvar = "weeks_listed"
weights = "market_cap"

for i in vars[3:]:
  average_w = weighted_average(quintiles,i,groupvar, weights,["q1","q2","q3","q4","q5"])

  x = average_w[groupvar]
  yvars =[ average_w["quintile2_weighted"], average_w["quintile3_weighted"], average_w["quintile4_weighted"], average_w["quintile5_weighted"]]
  xlabel = groupvar
  ylabel = ""
  title = "weighted average "+i+ " by "+ "market_cap"+ " quintiles"
  figname = "weighted average"+i+ "by"+ "market_cap"+ "quintiles"
  leg=["quintile 2","quintile 3","quintile 4","quintile 5"]

  plot_series(x,yvars,leg,xlabel,ylabel,figname,title)

#average_w["quintile1_weighted"],"quintile 1",


#%% Identify tokens with market weights in each quintile

# %%
overlap = pd.read_csv("buyer_seller_overlap.csv")
# %%plot average buy sell overlap 
