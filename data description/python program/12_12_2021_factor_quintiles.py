#By Guangye Cao 
#Dec 11, 2021
#This notebook takes cryptocurrency onchain and financial data as inputs. It divides the data into quintiles based on the values of a specific factor at the end of each week, then calculate the market cap weighted average returns of each portfolio over the following week. 

#%%
import requests,csv
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt 
from matplotlib.backends.backend_pdf import PdfPages
#import sklearn 
import statsmodels
from datetime import datetime, date
import json
from scipy.stats import mstats
from scipy.stats import describe
import statsmodels.api as sm
import lxml

#%% Load raw data
pd.options.display.float_format = '{:,.4f}'.format

#file path
inputPath = "/Users/guangyecao/dissertation_empirical/data description/input/"

df = pd.read_csv(inputPath+"onchain_market.csv")
#%%  quintiles for number of growth of number of transactions

#for each token, calculate weekly growth in num_transaction
