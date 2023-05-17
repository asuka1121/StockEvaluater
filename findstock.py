"""

//CreateStockList 2nd edition // 

What we need to look at:
Revenue/Sales
Net Income
Shareholder equity - how much return if company goes boom
Shares outstanding -how much stock out there
Earnings per share (EPS)
Return on equity (ROE) - how efficient is the company 

Disqualified:
1.The two most recent quarterly reports and most recent annual report can not have negative net income.
2.The stock can not be experiencing a correction. 
3.Over half of the data can not be missing from the financial reports downloaded
4.uses foreign currency

Tests:
Annual reports must have an EPS increase of at least 20%
Annual reports must have an ROE increase of at least 17%
Quarterly reports must have an EPS increase of at least 20%
Quarterly reports must have a Sales increase of at least 20%

There will be 4 different files/folder
*Results.csv summarizing the test results.
*Processed.csv keeps track of which stocks have been processed along with any error messages.
Data folder includes data downloaded
Processed folder will contain condensed financial reports. 

Results: [stock, slope, averagePrice, annualeps, annualROE, quarterlyEPS, quarterlyrevenue, numfailed]

Processed folder: each file[fiscalDateEnding, totalRevenue, netincome, totalShareholderEquity, CommonStockShareOutstanding
, eps, roe, totalRevenuePercentChange, netIncomePercentChange, epsPercentChange]

Slope =  way of measuring how much the stock price has been advancing recently  

Full workflow:

workflow:
Download the data
for stock in all_stocks
    Run preliminary tests on stock (check if stock is disqualified)
    Calculate additional metrics
    Run main tests
    Save financial data for stock
Save results for all stocks

Functions needed: 
getallsymbol()- get list of symbol for all stock
getreport()- get financial report for symbol given // store in data folder
getFeature()- get the specific feature of each each symbol in the data file,  eg. total revenue
CreateTable()- create table suitable for csv files, given a number of features and data. 
ProccessedData()- create a file in the proccessed folder, to store proccesed data of each symbol
preTest()- returns a boolean to see if stock passes the initial check
UpdateResult()- create a csv file if doesnt exist, and update the result file
UpdateProcessed()- create a csv file if doesnt exist, and update the proccessed file
SaveAll()- save the result
RevPC()- calculate total revenue percentage change
NIPC()- calculate Net income percentage change
epsPC()- calculate earning percentage change
Slope()- calculate slope
"""
from twelvedata import TDClient
import requests
import yfinance as yf
import csv
import json
import bs4
import os
import pandas as pd 
import time
from datetime import datetime as dt
from datetime import timedelta as td
import copy
import warnings
import re
#return a list of symbols for stocks.
AlphaVantageAPI = "G4ODBOCA7V6ZTMEE"
def getallsymbol():
    td = TDClient(apikey="d25d8e167acb4f98a806f379facd5b22")
    exchange = "NASDAQ"
    country = "United States"
    url = f"https://api.twelvedata.com/stocks?exchange={exchange}&country={country}&apikey=d25d8e167acb4f98a806f379facd5b22"
    # Initialize client - apikey parameter is requiered
    
    response = requests.get(url).json()
    list = []
    for i in range(len(response["data"])):
        sym = response["data"][i]['symbol']
        list.append(sym)
    return list
#append a row for csv files. features = list of same vector. 
def addRow(filename, features):
    with open(filename, "a", newline="") as fd:
        writerObj = csv.writer(fd, lineterminator="\n")
        writerObj.writerow(features)
#get balance and income statemate //both annual and quarterly
#then store it in each folder
def getReportUS(symbol, folder, key):
    createFolder(symbol, folder)
    i = gather(symbol, 'INCOME_STATEMENT', key)
    b = gather(symbol, 'BALANCE_SHEET', key)
    time.sleep(0.5)

    assert 'Information' not in i.keys(), ('Max number of API calls reached for the day: 500 calls.')

    if (
            i == {}
            or b == {}
            or list(i.keys()) == ['Error Message']
            or list(b.keys()) == ['Error Message']
            or b['annualReports'] == []
            or i['annualReports'] == []
            ):
        return 'API failed'
    
    dfIncomeQuarterly = pd.DataFrame.from_dict(i['quarterlyReports'])
    dfIncomeAnnual = pd.DataFrame.from_dict(i['annualReports'])
    dfBalanceQuarterly = pd.DataFrame.from_dict(b['quarterlyReports'])
    dfBalanceAnnual = pd.DataFrame.from_dict(b['annualReports'])

    newFolderPath = folder+f"\{symbol}"
    saveFile(symbol, 'Quarterly Income Statement', dfIncomeQuarterly, newFolderPath)
    saveFile(symbol, 'Annual Income Statement', dfIncomeAnnual, newFolderPath)
    saveFile(symbol, 'Quarterly Balance Sheet', dfBalanceQuarterly, newFolderPath)
    saveFile(symbol, 'Annual Balance Sheet', dfBalanceAnnual, newFolderPath)

    



#get http request for alphavantage 
#url = 'https://www.alphavantage.co/query?function=BALANCE_SHEET&symbol=IBM&apikey=demo'
def gather(symbol, type, key):
    apikey = key
    baseurl = 'https://www.alphavantage.co/query?function='
    try:
        response = requests.get(baseurl + type + '&symbol=' + symbol + '&apikey=' + apikey)
        try:
            soup = bs4.BeautifulSoup(response.content, 'html.parser')
            return json.loads(str(soup))
        except:
            return {}
    except:
        return {}

#save file in what ever folder
def saveFile(s, fileType, df, folder):
    f = s + ' ' + fileType + '.csv'
    path = os.path.join(folder, f)
    df.to_csv(path, index=False)

#create a folder in a folder
def createFolder(s, folder):
    if not os.path.exists(folder + f"\{s}"):
        os.mkdir(folder + f"\{s}") 

#check if file exist
def fileExist(sym):
    folderpath = "data" + f"\{sym}"
    return os.path.exists(folderpath)
    

#loop through each symbols
#If symbol doesnt exist add row
#If exist check status and update status
def UpdateProccesed(symbol, errormessage, proccesedFile):
    # opening the CSV file
    temp = pd.read_csv(proccesedFile)

    with open(proccesedFile, 'r') as f:
    # reading the CSV file
        reader = csv.reader(f, delimiter=',') 
        
        # iterating the rows
        for row in reader:
            if row == []:
                continue
            if symbol == row[0]:
                #replace the row with the new info [symbol, errormessage]
                    
                for i in range(temp.shape[0]):
                    if(row[0] == temp.iloc[i, 0]):
                        temp.iloc[i,1] = errormessage   
                        temp.to_csv(proccesedFile, index=False)
                        return
        list = [symbol, errormessage]
        addRow(proccesedFile, list)    
        return
    
def saveResult(record, file):
    with open(file, 'a', newline="") as csvfile:
        fieldnames  = ['stock', 'slope', 'avgpc','annualeps', 'annualroe', 'quarterlyeps','quarterlyrevenue', 'numfailed']
        writer = csv.DictWriter(csvfile,fieldnames=fieldnames, lineterminator="\n")
        df = pd.read_csv(file) # or pd.read_excel(filename) for xls file
        if(df.empty):
            writer.writeheader()
        writer.writerow(record)
#class for stock to hold all info for each stocks
class Stock():

    def __init__(self, stock, dataFolder):
        """Initialize."""
        self.dataPath = dataFolder
        self.processPath = 'Processed'
        self.s = stock
        self.ab = self.read('Annual Balance Sheet')
        self.ai = self.read('Annual Income Statement')
        self.qb = self.read('Quarterly Balance Sheet')
        self.qi = self.read('Quarterly Income Statement')
        self.miss = {'a': None, 'q': None}
        self.nothingMissing = {'totalRevenue': [],
                               'netIncome': [],
                               'totalShareholderEquity': [],
                               'commonStockSharesOutstanding': []}
        self.record = {'stock': self.s, 'slope': 0, 'avgpc': 0,
                       'annualeps': 0, 'annualroe': 0, 'quarterlyeps': 0,
                       'quarterlyrevenue': 0, 'numfailed': 0}
        self.errorMessage = 'processed'
        self.daysClosed = [dt(2022, 1, 1), dt(2022, 1, 20), dt(2022, 2, 17),
                           dt(2022, 4, 10), dt(2022, 5, 25), dt(2022, 7, 3),
                           dt(2022, 9, 7), dt(2022, 11, 26), dt(2022, 12, 25),
                           dt(2023, 1, 1), dt(2023, 1, 18), dt(2023, 2, 15),
                           dt(2023, 4, 2), dt(2023, 5, 31), dt(2023, 7, 5),
                           dt(2023, 9, 6), dt(2023, 11, 25), dt(2023, 12, 24)]


    #check if stock is qualified
    def test(self, report, r, mineps=0.2, minrev=0.2, minroe=0.17):
        """
        Check if stocks pass percent change tests.

        1. Annual - EPS must increase by 20%
        2. Annual - ROE must be over 17%
        3. Quarterly - EPS must increase by 20%
        4. Quarterly - Sales must increase by 20%
        """
        m = 'annual' if r == 'a' else 'quarterly'
        if report.loc[0, 'epsPercentChange'] < mineps:
            self.record[m + 'eps'] = 1
            self.record['numfailed'] += 1
        if m == 'annual':
            if report.loc[0, 'roe'] < minroe:
                self.record['annualroe'] = 1
                self.record['numfailed'] += 1
        if m == 'quarterly':
            if report.loc[0, 'totalRevenuePercentChange'] < minrev:
                self.record['quarterlyrevenue'] = 1
                self.record['numfailed'] += 1

        None


    #read the report of the stocks
    def read(self, report):
        file = self.s + ' ' + report + '.csv'
        folderpath = self.dataPath + f"\{self.s}"
        path = folderpath +f"\{file}"
        dataframe = pd.read_csv(path)
        return dataframe
    
    """Check the modified average slope of the stock price."""
    def checkSlope(self):
        def getBackYearMonth(y, m):
            """Get year and month by subtracting one month."""
            if m == 1:
                y -= 1
                m = 12
            else:
                m -= 1
            return dt(y, m, 1)
        
        # Get a list of days to use the stock price
        dates = []
        today = dt.now()
        year = today.year
        month = today.month

        # If the day of the month is greater than 10, calculate the slope of
        # the current month. Otherwise, calculate the slop from today to the
        # first day of the previous month
        if today.day > 10:
            dates.append(dt(year, month, 1))

        back = getBackYearMonth(year, month)
        dates.append(back)

        #get range of dates
        while len(dates) < 6:
            back = getBackYearMonth(back.year, back.month)
            dates.append(back)

        dates.sort()

        df = yf.download(self.s, period='1y', interval='1d')
        # Get the most recent close price
        last = df.iloc[-1, 3]
        self.changes = []
        adjSlope = 0
        normalizer = 0

        for i in range(len(dates)):
            # Fist of the month might not be a day that the stock market is
            # open. If it is not then subtract one day until it is in df.index
            d = dates[i]
            while d not in df.index or d in self.daysClosed:
                d -= td(days=1)
            # Get close price
            price = df.loc[d, 'Close']
            # Calculate percent change
            pc = (last - price) / price
            # Weight increases each month. More weight is added to recent slopes
            weight = i + 1
            # Add to normalizer, this will normalize the cumulative slopes.
            normalizer += weight
            # Add to the cumulative slope
            adjSlope += pc * (weight)
            self.changes.append(pc)

        self.record['slope'] = round(adjSlope / normalizer, 3)

        if adjSlope < 0:
            self.errorMessage = 'slopefailed'
    def prelimTests(self):
        """Prepare reports and preform preliminary tests."""

        def removeExcess(b, i, rows, errorMessage='none'):
            """Remove excess rows."""
            minRows = len(b) if len(b) < rows else rows
            b = b.iloc[:minRows, :]
            i = i.iloc[:minRows, :]
            try:
                if not all(b['fiscalDateEnding'] == i['fiscalDateEnding']):
                    return None, None, None, True
            except:
                self.errorMessage = "idk"
            return b, i, minRows, False

        def checkPercentNone(b, i, minRows, nonePercent=0.5):
            """Check the percentage of rows that are 'None'."""
            for c in ['totalShareholderEquity', 'commonStockSharesOutstanding']:
                numNone = b[b[c] == 'None'].index.size
                if numNone / minRows > nonePercent:
                    return True
            for c in ['netIncome', 'totalRevenue']:
                numNone = i[i[c] == 'None'].index.size
                if numNone / minRows > nonePercent:
                    return True
            return False

        # Remove excess number of rows
        self.ab, self.ai, minRowsA, unaligna = removeExcess(self.ab, self.ai, 4)
        self.qb, self.qi, minRowsQ, unalignq = removeExcess(self.qb, self.qi, 14)

        if unaligna or unalignq:
            self.errorMessage = 'unaligned reports'
            return

        # Check the percentage of rows that are missing data
        tooManyNoneA = checkPercentNone(self.ab, self.ai, minRowsA)
        tooManyNoneQ = checkPercentNone(self.qb, self.qi, minRowsQ)

        if tooManyNoneA or tooManyNoneQ:
            self.errorMessage = 'data missing'
            return
    #check if have negative income
    def checkNegativeIncome(self):
        def getIncome(report, i, period):
            income = report.loc[i, 'netIncome']
            if income in ['None', '0', 0]:
                income = 0
            return income

        self.qi.loc[0, 'netIncome'] = getIncome(self.qi, 0, 'quarterly')
        self.qi.loc[1, 'netIncome'] = getIncome(self.qi, 1, 'quarterly')
        self.ai.loc[0, 'netIncome'] = getIncome(self.ai, 0, 'annual')

        for income in [self.qi.loc[0, 'netIncome'],
                       self.qi.loc[1, 'netIncome'],
                       self.ai.loc[0, 'netIncome']]:
            # User can input 0 if the data does not exist. In which case
            # update the error message
            if income == 0:
                self.errorMessage = 'data does not exist'
                return
            if int(income) < 0:
                self.errorMessage = 'negative net income'
                return

    #calculate ROE and EPS
    def calculate(self, report):
        """Calculate ROE and EPS."""
        #use formula
        report['eps'] = report['netIncome'] / report['commonStockSharesOutstanding']
        report['roe'] = report['netIncome'] / report['totalShareholderEquity']
        return report


    def percentChange(self, t2, r):
        """Calculate percentage change of sales and EPS."""
        p = 4 if r == 'q' else 1
        t1 = t2.shift(periods=-p)
        for c in ['totalRevenue', 'netIncome', 'eps']:
            t2[c + 'PercentChange'] = (t2[c] - t1[c]) / t1[c].abs()
        return t2


    def reduceDF(self):
        """
        Combine balance sheet and income statements.

        Earnings per share (EPS) is calculated as a company's profit divided by
        the outstanding shares of its common stock

        Shareholder equity (SE) is the corporation's owners'residual claim on
        assets after debts have been paid.
        ...  Shareholders’ equity = total assets − total liabilities

        Return on equity (ROE) is a measure of financial performance calculated
        by dividing net income by shareholders' equity. Because shareholders'
        equity is equal to a company’s assets minus its debt, ROE is considered
        the return on net assets.
        """

        def reduce(i, b):
            """Reduce dataframe."""
            report = pd.concat([i['fiscalDateEnding'],
                                i['totalRevenue'],
                                i['netIncome'],
                                b['totalShareholderEquity'],
                                b['commonStockSharesOutstanding']],
                               axis=1, ignore_index=True)
            report.columns = ['fiscalDateEnding', 'totalRevenue',
                              'netIncome', 'totalShareholderEquity',
                              'commonStockSharesOutstanding']
            return report

        self.reports = {'a': reduce(self.ai, self.ab),
                        'q': reduce(self.qi, self.qb)}







    



def preliminaryTests(stock):
    """Run preliminary tests to see if stock failed and can be skipped."""
    # Perliminary tests to disqualify a stock
    stock.prelimTests()

    # Skip stock if it failed any of the preliminary tests
    if stock.errorMessage != 'processed':
        return

    # Check for negative income
    stock.checkNegativeIncome()

    # Skip stock if it has negative net income
    if stock.errorMessage != 'processed':
        return

    # Check if stock price has been decreasing recently
    stock.checkSlope()

    # Skip stock if it does not have increasing price change
    if stock.errorMessage != 'processed':
        return
    
    stock.reduceDF()

def analyzeStock(stock, r):
    """Run important tests to determine if quality stock."""
    # Calculate ROE and EPS
    stock.reports[r] = stock.calculate(stock.reports[r])

    # Calculate the percentage change year over year
    stock.reports[r] = stock.percentChange(stock.reports[r], r)

    # final check
    stock.test(stock.reports[r], r)

def getSymbolFromList(filename):
    list = []
    dataframe = pd.read_csv(filename)
    i = 1
    for i in range(dataframe.shape[0]):
        tempSym = dataframe.iloc[i, 0] 
        list.append(tempSym)
    return list
import random

#symlist = getallsymbol()
fileName = "proccessed.csv"
#key = ["symbol" , "proccesed"]
#addRow(fileName, key)
stocks = getSymbolFromList("nasdaqStrongBuyTech.csv")
"""
for i in range(5):
    randind = random.randint(0, len(symlist))
    stocks.append(symlist[randind])
print(stocks)

"""
for sto in stocks:
    time.sleep(60)
    UpdateProccesed(sto, "proccessed", "proccessed.csv")
    try:
        getReportUS(sto, "data", AlphaVantageAPI)
    except:
        continue
    stock = Stock(sto, "data")

    
    preliminaryTests(stock)
    if stock.errorMessage != 'processed':
        UpdateProccesed(stock.s, stock.errorMessage, "proccessed.csv" )
        continue

    #update report for both annual and quarterly.
    for r in stock.reports:
        analyzeStock(stock, r)

    createFolder(sto, "processedData")
    newPath = "processedData"+f"\{sto}"

    saveFile(sto, "finalAnnual", stock.reports["a"], newPath)
    saveFile(sto, "finalquarterly", stock.reports["q"], newPath)

    saveResult(stock.record,"results.csv")
    