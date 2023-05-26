""" Get info on stock and create a csv file """

import pandas as pd
import requests
import yfinance as yf

def getSymbolFromList(filename):
    list = []
    dataframe = pd.read_csv(filename)
    i = 1
    for i in range(dataframe.shape[0]):
        tempSym = dataframe.iloc[i, 0] 
        list.append(tempSym)
    return list
stocks = getSymbolFromList("results.csv")
sortby = "currentPrice"
my_columns = ['Symbol','longName', 'industry', 'longBusinessSummary','currentPrice', 'fiftyTwoWeekLow', 'fiftyTwoWeekHigh', 'dividendYield','revenueGrowth', 'website']
final_dataframe = pd.DataFrame(columns = my_columns)

for sym in stocks:
    company = yf.Ticker(sym)
    data = company.info
    col2=data["longName"]
    col3=data["industry"]
    col4=data["longBusinessSummary"]
    col5=data["currentPrice"]
    col6=data["fiftyTwoWeekLow"]
    col7=data["fiftyTwoWeekHigh"]
    try:
        col8=data["dividendYield"]
    except:
        col8="None"
    col9=data["revenueGrowth"]
    col10=data["website"]
    final_dataframe.loc[len(final_dataframe.index)] = [sym,col2,col3,col4,col5,col6,col7,col8,col9,col10]
    
final_dataframe.sort_values(f'{sortby}', ascending=True, inplace=True)
final_dataframe.reset_index(drop = True, inplace = True)
path = "resultInfo.csv"
final_dataframe.to_csv(path, index=False)

