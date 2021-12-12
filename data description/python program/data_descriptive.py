#%%
import requests,csv
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt 
from matplotlib.backends.backend_pdf import PdfPages
import matplotlib.dates as mdates
import sklearn 
import statsmodels
from datetime import datetime, date
import json
from scipy.stats import mstats
from scipy.stats import describe
import statsmodels.api as sm
import lxml

#6 decimal points
pd.options.display.float_format = '{:,.8f}'.format

onchain_market = pd.read_csv("onchain_market.csv")
df = onchain_market
df["date"] = pd.to_datetime(df["date"])

'''
df.dtypes
df.describe(include="all")
df.info()
'''
df['max_supply'] = df["max_supply"].str.replace(',', '').astype("float64")

#%% Plot of the number of tokens each month 
token_count = df.groupby(pd.Grouper(key="date",freq='M')).nunique()
token_count = token_count[["token_address","cmc_url"]]
token_count_plot = token_count.plot(legend=["erc20", "total"], title = "Number of tokens, monthly")
fig = token_count_plot.get_figure()
fig.savefig("token_count.jpg")
# %% total market_cap, bitcoin, ethereum, erc_20
# check for duplicates
results= df.groupby(['token_address','date']).size()
results = results[results>1]
results = results.reset_index()
results["token_address"].unique()  #list of token addresses with duplicate dates. 

#%%
check= df.groupby(['token_address'])['cmc_url'].nunique()
check1 = check[check>1]
check1
#%%
#somehow DigitalBits, Krios and 3 others ended up with wrong contract addresses, so delete these two for now. 
index = df[(df["cmc_url"]=="/currencies/digitalbits/")|(df["cmc_url"]=="/currencies/krios/")
            |(df["cmc_url"]=='/currencies/crpt/')|(df["cmc_url"]=='/currencies/cryptobuyer/')
            |(df["cmc_url"]=='/currencies/waletoken/')].index
df = df.drop(index,axis=0)
'''
|(df["cmc_url"]=="/currencies/nsure-network/")
            |(df["cmc_url"]=='/currencies/origin-dollar/')|(df["cmc_url"]=='/currencies/contentbox/')
'''

#%% Calculate and plot market caps:total, bitcoin, ethereum, tether, ERC20
#df2 = df.set_index(['cmc_url','date'])
#df2=df2.unstack(level=0) 

# includes ALL tokens everyday

df1 = df[["date","cmc_url","market_cap","token_address"]]
df1 = df1.set_index(['cmc_url','date'])
df2 = df1.unstack(level=0) 

df2.index = pd.to_datetime(df2.index)
df2.columns = df2.columns.rename(['variables',"cmc_url"])
df_market = df2.xs('market_cap',  level="variables", axis=1)
df_market = df_market.dropna(how="all")
df_market = df_market.loc[df_market.index<="2021-03-03"]
#total market cap
df_market["total_MC"] = df_market.sum(axis=1)
total_MC=df_market["total_MC"].to_frame()

#separate dataset for erc20 tokens
df_erc = df1.loc[df1.token_address.notnull()]
#df_erc = df_erc.set_index(['cmc_url','date'])
df_erc = df_erc.unstack(level=0) 
df_erc.index = pd.to_datetime(df_erc.index)
df_erc.columns = df_erc.columns.rename(['variables',"cmc_url"])
df_ercMC = df_erc.xs('market_cap',  level="variables", axis=1)
df_ercMC = df_ercMC.loc[df_ercMC.index<="2021-03-03"]
df_ercMC["ERC20_MC"] = df_ercMC.sum(axis=1)
ERC20_MC = df_ercMC["ERC20_MC"].to_frame()

#bitcoin and ethereum
bitcoin_MC =df_market["/currencies/bitcoin/"].to_frame()
ethereum_mc = df_market["/currencies/ethereum/"].to_frame()

# combine total, bitcoin, ethereum, and erc-20 together and plot
A = bitcoin_MC.merge(ethereum_mc,how="outer", on="date")
B = A.merge(ERC20_MC,how="outer", on="date")
CMC_chart = B.merge(total_MC,how="outer", on="date")
CMC_chart = CMC_chart.replace(np.nan,0)
CMC_chart["other"] = CMC_chart["total_MC"] - CMC_chart["ERC20_MC"] - CMC_chart["/currencies/bitcoin/"] - CMC_chart["/currencies/ethereum/"]

#market cap level plot
plt.stackplot(CMC_chart.index,CMC_chart["ERC20_MC"], CMC_chart["/currencies/bitcoin/"], CMC_chart["/currencies/ethereum/"], CMC_chart["other"], labels=['ERC 20','Bitcoin','Ethereum',"Other"])
plt.legend(loc='upper left')
plt.title("Market Capitalization")
plt.savefig("Market_Cap")
plt.close()
# market cap percentage chart
chart_perc = CMC_chart.div(CMC_chart["total_MC"],axis=0)
plt.stackplot(chart_perc.index,chart_perc["ERC20_MC"], chart_perc["/currencies/bitcoin/"], chart_perc["/currencies/ethereum/"], chart_perc["other"], labels=['ERC 20','Bitcoin','Ethereum',"Other"])
plt.legend(loc='lower left')
plt.title("Market Capitalization%")
plt.savefig("Market_Cap %")

#%% Construct a "market return" and "market volatility" for crypto
# need $1000 in trading volume
df_nonStable = df.loc[df["Stablecoin"]!=1]
#df: 1,654,146 by 87
#df_market: 1,180,494 by 6

df_market = df_nonStable[["date","cmc_url","token_address","close","market_cap","volume"]]

#calculate price index for the market
#to be included, volume must be >1000
df_market.market_cap = df_market.market_cap.replace(np.nan,0)
df_market.volume = df_market.volume.replace(np.nan,0)
df_market = df_market.loc[df_market.volume>1000] #delete tokens-days with less than $1000 in trading volume

df_market = df_market.set_index(['cmc_url','date'])
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
#market volatility: 

#%% Save market index
df_price = df_price.reset_index()
market_index = df_price[["date", "price_index", "count"]]
market_index.to_csv("market_index.csv", index=False)
#%%
#plot crypto market index with number of tokens included, without stablecoins
fig, ax1 = plt.subplots()
plt.title("Crypto market price index vs # of tokens")
ax2 = ax1.twinx()
ax1.plot(df_price.index, df_price["price_index"], 'g-')
ax2.plot(df_price.index, df_price["count"], 'b-')

ax1.set_xlabel('Date')
ax1.set_ylabel('Price Index, $', color='g')
ax2.set_ylabel('# tokens', color='b')
ax1.format_xdata = mdates.DateFormatter('%Y-%m')
ax2.format_xdata = mdates.DateFormatter('%Y-%m')
fig.savefig("crypto_index")
plt.show()

#plt.close()


#Plot market return and market volatility:
# daily
df_price["market_return"] = np.log(df_price["price_index"]) - np.log(df_price["price_index"].shift(1))
plt.plot(df_price.index, df_price["market_return"])
plt.title("Crypto Market Return, daily")
plt.xlabel('Date')
plt.ylabel('Log Return')
plt.savefig("crypto_market_return_day")
plt.close()

df_price["rolling_vol"] = df_price["market_return"].rolling(30).std()
plt.plot(df_price.index, df_price["rolling_vol"])
plt.title("Crypto Market 30-day Return Standard Deviation")
plt.xlabel('Date')
plt.ylabel('30-day Rolling Return Standard Deviation')
plt.savefig("crypto_market_vol")
plt.close()

#weekly
df_priceW = df_price.resample('W').apply({"price_index":'last'})
df_priceW["week_return"] = np.log(df_priceW["price_index"]) - np.log(df_priceW["price_index"].shift(1))
plt.plot(df_priceW.index, df_priceW["week_return"])
plt.title("Crypto Market Return, weekly")
plt.xlabel('Date')
plt.ylabel('Log Return')
plt.savefig("crypto_market_return_week")
plt.show()
plt.close()

#%%table of distribution of weekly ERC-20 & non ERC-20 return and volatility
df1 = df[["date","cmc_url","close","volume","token_address","Stablecoin"]]
df1 = df1.loc[df1["volume"]>1000]
df1 = df1.loc[df1["Stablecoin"]!=1]
df1 = df1.loc[df1["date"]<"2021-03-04"]
df1 = df1.reset_index()
df1 = df1.set_index("date")
df_erc = df1.loc[df1.token_address.notnull()]
df_nonerc = df1.loc[df1.token_address.isnull()]
df_btc = df1.loc[df1["cmc_url"]=="/currencies/bitcoin/"]
df_eth = df1.loc[df1["cmc_url"]=="/currencies/ethereum/"]
#initiate empty Dataframe for each stat
'''
mean = pd.DataFrame(index = df_erc.index, columns=["df_erc", "df_nonerc", "df_btc","df_eth"])
vol = pd.DataFrame(index = df_erc.index, columns=["df_erc", "df_nonerc", "df_btc","df_eth"])
kurtosis = pd.DataFrame(index = df_erc.index, columns=["df_erc", "df_nonerc", "df_btc","df_eth"])
skewness = pd.DataFrame(index = df_erc.index, columns=["df_erc", "df_nonerc", "df_btc","df_eth"])
'''
#%%
A = [df_erc,df_nonerc,df_btc]
#A = [df_erc,df_nonerc,df_btc,df_eth]
#cols = ["erc-20", "non-erc20", "btc","eth"]
cols = ["erc-20", "non-erc20", "btc"]

for i in range(len(A)):
    data = A[i].reset_index()
    data= data.set_index(['cmc_url','date'])
    data = data.unstack(level=0) ['close']  #price only
    data.index = pd.to_datetime(data.index)

    data = data.resample("W").last()
    data = data.apply(lambda x: (np.log(x)).diff())
    
    if i==0:
        mean =          pd.DataFrame(index = data.index, columns=["erc-20", "non-erc20", "btc"]) #,"eth"
        mean[cols[i]] = data.mean(axis=1)
        skewness =      pd.DataFrame(index = data.index, columns=["erc-20", "non-erc20", "btc"])#,"eth"
        skewness[cols[i]]= data.skew(axis=1)
        kurtosis =      pd.DataFrame(index = data.index, columns=["erc-20", "non-erc20", "btc"])#,"eth"
        kurtosis[cols[i]] = data.kurt(axis=1)
        vol =           pd.DataFrame(index = data.index, columns=["erc-20", "non-erc20", "btc"])#,"eth"
        vol[cols[i]]    = data.std(axis=1)
    else:    
        mean[cols[i]] = data.mean(axis=1)
        skewness[cols[i]]= data.skew(axis=1)
        kurtosis[cols[i]] = data.kurt(axis=1)
        vol[cols[i]]    = data.std(axis=1)

mask = (mean.index<"08-22-2016") & mean["erc-20"].notnull()
mean["erc-20"][mask]=np.nan

#%% plot the above tables
fig=plt.figure()
mean.plot(title="Average weekly return", style=['-','-','-.'])
plt.tight_layout()
plt.savefig("mean_return.pdf",bbox_inches='tight')
plt.close()

fig=plt.figure()
vol.plot(title="Weekly return standard deviation", style=['-','-','-.'])
plt.tight_layout()
plt.legend(["ERC-20","non ERC-20"])
plt.savefig("std_return.pdf",bbox_inches='tight')
plt.close()

fig=plt.figure()
kurtosis.plot(title="Weekly return kurtosis", style=['-','-','-.'])
plt.tight_layout()
plt.legend(["ERC-20","non ERC-20"])
plt.savefig("kurtosis_return.pdf",bbox_inches='tight')
plt.close()

fig=plt.figure()
skewness.plot(title="Weekly return skewness", style=['-','-','-.'])
plt.tight_layout()
plt.legend(["ERC-20","non ERC-20"])
plt.savefig("skewness_return.pdf",bbox_inches='tight')
plt.close()

#%% For following analysis, drop low volume obsevation, stablecoin, restrict date range.
df1 = df.loc[df["market_cap"]>0]
df1 = df1.loc[df1["Stablecoin"]!=1]
df1 = df1.loc[(df1["date"]<"2021-03-04") & (df1["date"]>"08-22-2016" )]
#%% return by max supply
df_max = df1[["date","cmc_url","close","volume","token_address","max_supply"]]
max = df_max["max_supply"].notnull()
df_max["max"] = np.nan
df_max["max"][max] = 1
#%%
data = df_max
data= data.set_index(['cmc_url','date'])
data = data.unstack(level=0) [['close',"max"]]  #price only
data.index = pd.to_datetime(data.index)
#%%
data = data.resample("W").last()
data["close"] = data["close"].apply(lambda x: (np.log(x)).diff())
cols = data["max"].columns.tolist()
data["max"] = data["max"].ffill().bfill()
#%%
df_max = data["close"]*data["max"].values
df_nomax = data["close"][df_max.isnull()]
mean1 = df_max.mean(axis=1)
mean2 = df_nomax.mean(axis=1)

mean = pd.DataFrame(list(zip(data.index,mean1,mean2)),columns=["date","max_supply","no_max_supply"])
mean = mean.set_index("date")

fig=plt.figure()
mean.plot(title="average return of capped and uncapped supply tokens", style=['-','-.'])
plt.tight_layout()
plt.legend(["with supply cap","without supply cap"])
plt.savefig("supply_cap_return.pdf",bbox_inches='tight')
#%%return by industry
industry = ["cex", "defi","distributed_comp","entertainment","infrastructure","biz_solution"]
df_industry =df1[["date","cmc_url","close","volume","token_address","cex", "defi","distributed_comp","entertainment","infrastructure","biz_solution"]]

df_industry = df_industry.set_index(["cmc_url","date"])
df_industry = df_industry.unstack(level=0) [['close',"volume", "cex", "defi","distributed_comp","entertainment","infrastructure","biz_solution"]]  #price only
df_industry.index = pd.to_datetime(df_industry.index)
df_industry = df_industry.resample("W").last()
df_industry["close"] = df_industry["close"].apply(lambda x: (np.log(x)).diff())

#initate dataframe for mean, standard deviation, kurtosis and skewness
mean =  pd.DataFrame(index = df_industry.index, columns = industry)
vol =  pd.DataFrame(index = df_industry.index, columns = industry)
kurtosis = pd.DataFrame(index = df_industry.index, columns = industry)
skewness = pd.DataFrame(index = df_industry.index, columns = industry)

for i in industry:
    df_industry[i] = df_industry[i].ffill().bfill()
    data = df_industry["close"]*df_industry[i].values
    mean[i] = data.mean(axis=1)
    vol[i] = data.std(axis=1)
    skewness[i] = data.skew(axis = 1)
    kurtosis[i] = data.kurtosis(axis = 1)
    

fig=plt.figure()
kurtosis.plot(title="Return kurtosis by industry",style=['-','-','-.','-.',':',':'])
plt.tight_layout()
plt.legend(industry)
plt.savefig("kurtosis_industry.pdf",bbox_inches='tight')
plt.close()

fig=plt.figure()
mean.plot(title="Return Mean by industry",style=['-','-','-.','-.',':',':'])
plt.tight_layout()
plt.legend(industry)
plt.savefig("mean_industry.pdf",bbox_inches='tight')
plt.close()

fig=plt.figure()
vol.plot(title="Return standard deviation by industry",style=['-','-','-.','-.',':',':'])
plt.tight_layout()
plt.legend(industry)
plt.savefig("volatility_industry.pdf",bbox_inches='tight')
plt.close()

fig=plt.figure()
skewness.plot(title="Return skewness by industry",style=['-','-','-.','-.',':',':'])
plt.tight_layout()
plt.legend(industry)
plt.savefig("skewness_industry.pdf",bbox_inches='tight')
plt.close()

#%% summary of onchain variables
df1=df1.rename(columns={"bull/bear":"bull_bear"})
variables = ["date","cmc_url","close", "volume","market_cap",'positive_address_count',
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
 '1perc_buy_share',"token_address"]
df1=df1.rename(columns={"bull/bear":"bull_bear"})
onchain=df1[variables]

onchain = onchain.reset_index()
onchain = onchain.drop("index", axis=1)
onchain= onchain.loc[onchain.token_address.notna()]
#the following tokens do not have onchain data before March, 2021
onchain = onchain.loc[~onchain.token_address.isin(['0x20398ad62bb2d930646d45a6d4292baa0b860c1f',
 '0x298d492e8c1d909d3f63bc4a36c66c64acb3d695',
 '0x4a621d9f1b19296d1c0f87637b3a8d4978e9bf82',
 '0x635d081fd8f6670135d8a3640e2cf78220787d56'])]

#%% drop tokens with less than 180 days of price data
price_count = pd.DataFrame(onchain.groupby("cmc_url").close.count()).reset_index()
price_count = price_count.rename(columns={"close":"close_count"})
A = onchain.merge(price_count, on="cmc_url")
onchain = A.loc[A.close_count>180]

#drop tokens with less than 180 days of onchain data
address_count=pd.DataFrame(onchain.groupby("cmc_url").positive_address_count.count()).reset_index()
address_count = address_count.rename(columns={"positive_address_count":"address_count"})
B = onchain.merge(address_count, on="cmc_url")
onchain = B.loc[B.address_count>180]

#price_count = price_count[price_count["close_count"]<=180]
onchain.to_csv("onchain_stata2.csv",index=False)
#%%
# for each onchain variable, compute its correlation with price for each token, 
# then plot histogram of all token's correlations

for i in range(4,len(variables)-1):
    print(variables[i])
    plt.figure()
    onchain.groupby("cmc_url")[["close",variables[i]]].corr().unstack().iloc[:,1].plot(kind="kde",title=variables[i])
    plt.savefig(variables[i]+"price_corr")
    plt.show()
    plt.close()

# plot correlation of onchain data with return
    
#%% for each token regress the onchain variables on price, keep the significant ones, and plot them


#results = pd.DataFrame(columns = ['coef', 'std err', 't', 'P>|t|','[0.025', '0.975]']) 

def regLinear(data, xvars,yvar,coeff):
    y = data[yvar]
    #print(len(y))
    x = data[xvars]
    x = sm.add_constant(x)
    #print(len(x))
    model = sm.OLS(y,x,missing='drop')
    results = model.fit()
    dd = pd.read_html(results.summary().tables[1].as_html(),header=0,index_col=0)[0]
    #print(dd)
    coeff = pd.concat([coeff,dd])
    return coeff

#%%
coeff = pd.DataFrame(columns = ['coef', 'std err', 't', 'P>|t|','[0.025', '0.975]']) 
for var in onchain.columns[4:-3]:
#var = onchain.columns[4]
    print(var)
    dat = onchain.loc[onchain[var].notna()]
    output  = dat.groupby("cmc_url").apply(regLinear, var,"close",coeff)
    output["var"] = var
    if var == "positive_address_count":
        results = output
    else:
        results = pd.concat([results,output])

results = results.reset_index()
results.to_csv("reg_results.csv", index=False)
#%% for each onchain variable, count the number tokens with significant coefficient, and the total number of tokens. 
#results.groupby(["var", "level_1"])["P>|t|"].apply(lambda x:len(x[x<0.05]))
results = pd.read_csv("reg_results.csv", index_col=0)

sig_count = pd.DataFrame(results.groupby(["var", "level_1"])["P>|t|"].apply(lambda x: x[x<0.05].count()))
sig_count = sig_count.reset_index()
sig_count = sig_count.rename(columns={"P>|t|":"sig_count"})

all_count = pd.DataFrame(results.groupby(["var", "level_1"])["P>|t|"].count())
all_count = all_count.reset_index()
all_count = all_count.rename(columns={"P>|t|":"total_count"})

results = results.merge(sig_count,on=["var","level_1"])
results = results.merge(all_count, on=["var","level_1"])

#%%for each onchain variable, calculate average coeff and 3 standard deviation boundaries 

#%% For each onchain variable, histogram of significant coefficients, with count of number of significant coeff and total count.
result_sig = results[results["P>|t|"]<=0.05]

#for each onchain variable, calculate average coeff and 3 standard deviation boundaries 

mean_coef = pd.DataFrame(result_sig.groupby(["var", "level_1"])["coef"].mean())
mean_coef = mean_coef.rename(columns={"coef":"mean_coef"})
std_coef = pd.DataFrame(result_sig.groupby(["var", "level_1"])["coef"].std())
std_coef = std_coef.rename(columns={"coef":"std_coef"})
result_sig = result_sig.merge(mean_coef,on=["var","level_1"])
result_sig = result_sig.merge(std_coef, on=["var","level_1"])

#%%
grouped = result_sig.groupby(["var",'level_1'])
'''
ncols=2
nrows = int(np.ceil(grouped.ngroups/ncols))
fig, axes = plt.subplots(nrows=nrows, ncols=ncols, figsize=(12,4), sharey=True)
for (key, ax) in zip(grouped.groups.keys(), axes.flatten()):
    grouped.get_group(key)["coef"].hist() #ax=ax
''' 
def sig_plot(grouped,coef,sig_count,total_count):
    for keys in grouped.groups.keys():
        fig = plt.figure()
        grouped.get_group(keys)[coef].plot.hist(bins = 20, title=str(keys))
        sig = round(grouped.get_group(keys)[sig_count].mean())
        total = round(grouped.get_group(keys)[total_count].mean())
        mean = round(grouped.get_group(keys)["mean_coef"].mean(),2)
        sd = round(grouped.get_group(keys)["std_coef"].mean(),2)
        plt.legend(loc='upper left')
        leg = str(sig)+' / '+str(total)+"\n mean = "+ str(mean)+", sd = "+str(sd)
        plt.legend([leg])
        plt.show()

sig_plot(grouped,"coef","sig_count","total_count")

#%% too many outliers, winsorize the significant results then plot

def bounds(dat, grouped, coef,low, high):
    dat["lb"] = grouped[coef].transform("quantile",low)
    dat["ub"] = grouped[coef].transform("quantile",high)
    return dat

def winsor(dat,coef,lb_var,ub_var):
    dat = dat.loc[(dat["coef"]>=result_sig[lb_var]) & (dat["coef"]<=dat[ub_var])]
    return dat
    
A = bounds(result_sig,grouped,"coef",0.025,0.975) 
result2 = winsor(A,"coef","lb","ub")
    
#result2 = result_sig.loc[(result_sig["coef"]>=result_sig["[0.025"]) & (result_sig["coef"]<=result_sig["0.975]"])]
grouped2 = result2.groupby(["var",'level_1'])

sig_plot(grouped2, "coef","sig_count","total_count")

#%% table of monthly return distribution

data = df.loc[df["volume"]>1000]
data = data.loc[data["Stablecoin"]!=1]
data = data.loc[data["date"]<"2021-03-04"][["date","cmc_url","close"]]

def monthly_return(df,var):
  data = df[:]
  #monthly log difference

  data["date"] = pd.to_datetime(data["date"])
  data = data.set_index("date")
  data = data.groupby("cmc_url").resample('M').last()
  data = data.drop("cmc_url",axis=1)
  data["monthly_return"] = data.groupby("cmc_url")[var].apply(lambda x: ((np.log(x)).diff())*100)
  data = data.reset_index()
  
  return data

monthly = monthly_return(data,"close")
# %% descriptive table
describe = monthly.groupby("date")["monthly_return"].describe()
describe["kurtosis"] = monthly.groupby("date")["monthly_return"].apply(pd.DataFrame.kurt)
describe["skewness"] = monthly.groupby("date")["monthly_return"].skew()
describe = describe.reset_index()
describe["date"] = pd.to_datetime(describe["date"])
describe['year'] = describe["date"].dt.year
describe['month'] = describe["date"].dt.month

describe = describe.groupby(["year","month"]).last()

pd.options.display.float_format = '{:,.1f}'.format
describe = describe.drop("date",axis=1)
#describe = describe.reset_index()
describe.to_csv("descriptive.csv")
print(describe.to_latex(index = True, multirow = True))

#%%