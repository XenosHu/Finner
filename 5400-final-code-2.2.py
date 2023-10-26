#!/usr/bin/env python
# coding: utf-8

# # 1. Set-up the environment
import asyncio
import nest_asyncio
import numpy as np
import os
import pandas as pd
import pandas_datareader as pdr
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from finnhub import Client as FinnhubClient
from pandas_datareader import DataReader
from plotly import colors, express, graph_objects, offline
from requests.exceptions import RequestException
import random
import requests
import string
import subprocess
import sys
import time


ticker_name = pd.DataFrame(pd.read_csv("nasdaq-listed.csv"))
ticker_name

nest_asyncio.apply()  

# API_KEY = 'cgn4mghr01qhveut0q00cgn4mghr01qhveut0q0g'
API_KEY = 'ch4vj6pr01quc2n51ud0ch4vj6pr01quc2n51udg'

async def get_data(symbol, start_date, end_date):
    url = f'https://finnhub.io/api/v1/stock/candle?symbol={symbol}&resolution=D&from={start_date}&to={end_date}&token={API_KEY}'
    try:
        response = await loop.run_in_executor(None, requests.get, url)
        data = response.json()
    except Exception as e:
        print(f"Error fetching data for symbol {symbol}: {e}")
        return None
    return symbol, data

async def main():
    symbols = ticker_name['Symbol'].tolist()
    
    now = time.strftime("%Y-%m-%d", time.localtime(time.time()))
    start_date = int(datetime.strptime('1980-01-01', '%Y-%m-%d').timestamp())
    end_date = int(datetime.strptime( now, '%Y-%m-%d').timestamp())
    
    tasks = [asyncio.create_task(get_data(symbol, start_date, end_date)) for symbol in symbols]
    results = await asyncio.gather(*tasks)
    
    combined_data = pd.DataFrame()
    for result in results:
        if result is not None and 't' in result[1] and 'o' in result[1] and 'h' in result[1] and 'l' in result[1] and 'c' in result[1] and 'v' in result[1]:
            symbol, data = result
            df = pd.DataFrame(data)
            df['t'] = pd.to_datetime(df['t'], unit='s')
            df['Symbol'] = symbol
            combined_data = pd.concat([combined_data, df], ignore_index=True)
    
    combined_data.rename(columns={'t': 'Date', 'o': 'Open', 'h': 'High', 'l': 'Low', 'c': 'Close', 'v': 'Volume'}, inplace=True)

    return combined_data

loop  = asyncio.get_event_loop()
combined_data = loop.run_until_complete(main())
combined_data


# # 3. EDGAR 10-K Filing and Logo URLs


get_ipython().system('pip install edgar')
get_ipython().system('pip install sec-api')

def generate_random_email():
    email_domain = "@example.com"
    random_string = ''.join(random.choices(string.ascii_letters + string.digits, k=10))
    return random_string + email_domain

headers = {'User-Agent': generate_random_email()}  

def exponential_backoff(attempt, max_wait=120):
    wait_time = min(2 ** attempt, max_wait)
    time.sleep(wait_time)
    
def get_most_recent_10k_url(cik, ticker):
    headers = {'User-Agent': generate_random_email()}
    filing_metadata_url = f'https://data.sec.gov/submissions/CIK{cik}.json'
    
    
    for attempt in range(10):
        try:
            filing_metadata = requests.get(filing_metadata_url, headers=headers)
            filing_metadata.raise_for_status()
            break
        except requests.exceptions.RequestException:
            exponential_backoff(attempt)
    else:
        print(f"Failed to fetch data for {ticker} after multiple attempts")
        return None

    all_forms = pd.DataFrame.from_dict(filing_metadata.json()['filings']['recent'])

    most_recent_10k_filtered = all_forms.loc[all_forms['form'] == '10-K']
    
    if most_recent_10k_filtered.empty:
        print(f"No 10-K form found for {ticker}")
        return None

    most_recent_10k = most_recent_10k_filtered.iloc[0]
    accession_number = most_recent_10k['accessionNumber'].replace('-', '')
    accession_number_hyphenated = most_recent_10k['accessionNumber']
    filing_date = most_recent_10k['filingDate']
    return f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession_number}/{accession_number_hyphenated}-index.html"

def get_cik_for_ticker(ticker):
    company_data = pd.DataFrame.from_dict(requests.get("https://www.sec.gov/files/company_tickers.json", headers=headers).json(), orient='index')
    company_data['cik_str'] = company_data['cik_str'].astype(str).str.zfill(10)
    result = company_data.loc[company_data['ticker'] == ticker]['cik_str'].values
    return result[0] if len(result) > 0 else None


# # 4. Companies' core info


def comp_info(ticker):
    API_KEY = 'NJVQJFLY9SSGTP55'
    url = f'https://www.alphavantage.co/query?function=OVERVIEW&symbol={ticker}&apikey={API_KEY}'
    response = requests.get(url)
    data = response.json()
    df = pd.DataFrame.from_dict(data, orient='index', columns=['Value'])
    return df

def info_core(ticker):
    df = comp_info(ticker)

    core = pd.DataFrame({
        'Name': df.loc['Name']['Value'],
        'Symbol': df.loc['Symbol']['Value'],
        'AssetType': df.loc['AssetType']['Value'],
        'Description': df.loc['Description']['Value'],
        'Exchange': df.loc['Exchange']['Value'],
        'Currency': df.loc['Currency']['Value'],
        'Country': df.loc['Country']['Value'],
        'Sector': df.loc['Sector']['Value'],
        'Industry': df.loc['Industry']['Value'],
        'Address': df.loc['Address']['Value'],
        'FiscalYearEnd': df.loc['FiscalYearEnd']['Value'],
        'LatestQuarter': df.loc['LatestQuarter']['Value'],
        'MarketCapitalization': df.loc['MarketCapitalization']['Value'],
        'EBITDA': df.loc['EBITDA']['Value'],
        'PERatio': df.loc['PERatio']['Value'],
        'PEGRatio': df.loc['PEGRatio']['Value'],
        'BookValue': df.loc['BookValue']['Value'],
        'DividendPerShare': df.loc['DividendPerShare']['Value'],
        'DividendYield': df.loc['DividendYield']['Value'],
        'EPS': df.loc['EPS']['Value'],
        'Beta': df.loc['Beta']['Value'],
        '52WeekHigh': df.loc['52WeekHigh']['Value'],
        '52WeekLow': df.loc['52WeekLow']['Value'],
        '50DayMovingAverage': df.loc['50DayMovingAverage']['Value'],
        '200DayMovingAverage': df.loc['200DayMovingAverage']['Value'],
        'SharesOutstanding': df.loc['SharesOutstanding']['Value']
    }, index=[0])

    core = core.transpose()
    return(core)

def info_other(ticker):
    df = comp_info(ticker)
    core = info_core(ticker)
    other = df.loc[~df.index.isin(core.columns)]
    other = other.transpose()
    return(other)


# # 5. Econ index from FRED using Postgresql

# In[11]:


get_ipython().system('pip install pandas-datareader')

df = pd.DataFrame()

def get_econ_index_data(start_date,end_date):

    start = datetime.datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.datetime.strptime(end_date, '%Y-%m-%d')

    date_range = pd.date_range(start, end, freq='D')
    df = pd.DataFrame(index=date_range)

    df['NASDAQ Composite Index'] = DataReader('NASDAQCOM', 'fred', start, end)
    df['CPI'] = DataReader('CPALTT01USM657N', 'fred', start, end)
    df['PPI'] = DataReader('PPIACO', 'fred', start, end)
    df['M1'] = DataReader('M1SL', 'fred', start, end)
    df['M2'] = DataReader('M2SL', 'fred', start, end)
    df['Unemployment Rate %'] = DataReader('UNRATE', 'fred', start, end)
    df['Real GDP'] = DataReader('GDPC1', 'fred', start, end)
    df['Real GDP Per Capita'] = DataReader('A939RX0Q048SBEA', 'fred', start, end)
    df['FED Effective Rate'] = DataReader('DFF', 'fred', start, end)
    df['PCE'] = DataReader('PCE', 'fred', start, end)
    df['U.S. Dollar Index'] = DataReader('DTWEXBGS', 'fred', start, end)
    df['Median Household Income'] = DataReader('MEHOINUSA672N', 'fred', start, end)

    df = df.fillna(method='ffill')
    df = df.fillna(method='bfill')

    return df


# # 6. Interactive UI using Flask

get_ipython().system('pip install flask')
get_ipython().system('pip install flask-ngrok')
get_ipython().system('pip install plotly')

get_ipython().system('pip install plotly>=5.0.0')
get_ipython().system('pip install --upgrade flask-ngrok')


os.environ['WERKZEUG_RUN_MAIN'] = 'true'

def get_data(symbol, start_date, end_date):
    api_key = 'cgn4mghr01qhveut0q00cgn4mghr01qhveut0q0g' 
    url = f'https://finnhub.io/api/v1/stock/candle?symbol={symbol}&resolution=D&from={start_date}&to={end_date}&token={api_key}'
    response = requests.get(url)
    return response.json()

def fetch_candlestick_data(ticker):
    end_date = int(time.time())
    start_date = int(time.mktime(time.strptime("01 01 1980", "%d %m %Y")))
    data = get_data(ticker, start_date, end_date)
    return data

def create_candlestick_chart(data, symbol):
    if 't' not in data or 'o' not in data or 'h' not in data or 'l' not in data or 'c' not in data:
        return None

    df = pd.DataFrame({'t': data['t'], 'o': data['o'], 'h': data['h'], 'l': data['l'], 'c': data['c']})
    df['t'] = pd.to_datetime(df['t'], unit='s')
    df['Date'] = df['t'].dt.strftime('%Y-%m-%d')

    fig = go.Figure(data=[go.Candlestick(x=df['Date'],
                                         open=df['o'],
                                         high=df['h'],
                                         low=df['l'],
                                         close=df['c'])])

    fig.update_layout(title=f"{symbol} Stock Prices",
                      xaxis_title="Date",
                      yaxis_title="Price ($)",
                      xaxis_rangeslider_visible=False
                      )

    chart_div = pyo.plot(fig, output_type='div', include_plotlyjs=False)

    return chart_div

def get_financial_data(ticker):
    cik = get_cik_for_ticker(ticker)
    if cik is None:
        print(f"Wrong ticker! {ticker}")
        return None, False

    url_10k = get_most_recent_10k_url(cik, ticker)
    logo_url = f'https://universal.hellopublic.com/companyLogos/{ticker}@2x.png'
    candlestick_data = fetch_candlestick_data(ticker)
    chart = create_candlestick_chart(candlestick_data, ticker)

    if url_10k is not None and chart is not None:
        return {'Symbol': ticker, 'CIK': cik, '10-K_URL': url_10k, 'Logo_URL': logo_url, 'Chart': chart}, True
    else:
        return None, True

def get_previous_close_price(ticker):
    api_key = 'cgn4mghr01qhveut0q00cgn4mghr01qhveut0q0g'
    url = f'https://finnhub.io/api/v1/quote?symbol={ticker}&token={api_key}'
    response = requests.get(url)
    if response.status_code == 200:
        stock_data = response.json()
        return stock_data['pc']
    else:
        return None
    
def get_real_time_stock_price(ticker):
    api_key = 'cgn4mghr01qhveut0q00cgn4mghr01qhveut0q0g'
    url = f'https://finnhub.io/api/v1/quote?symbol={ticker}&token={api_key}'
    response = requests.get(url)
    if response.status_code == 200:
        stock_data = response.json()
        return stock_data['c']
    else:
        return None

def create_econ_index_chart(df):
    fig = px.line(df, 
                  x=df.index, 
                  y=['NASDAQ Composite Index', 'CPI', 'PPI', 'M1', 'M2', 'Unemployment Rate %', 'Real GDP', 'Real GDP Per Capita', 'FED Effective Rate', 'PCE', 'U.S. Dollar Index', 'Median Household Income'],
                  title='Economic Index',
                  labels={'value': 'Value', 'index': 'Date'},
                  width=800, height=600)
    
    return fig.to_html(full_html=False, include_plotlyjs='cdn')
    
app = Flask(__name__, template_folder="my_templates")

@app.route('/', methods=['GET', 'POST'])
def index():
    error = None
    data = None
    core = None
    core_empty = True

    if request.method == 'POST':
        ticker = request.form['ticker'].upper()
        if not ticker:
            error = "Please enter a valid ticker symbol."
        else:
            with ThreadPoolExecutor(max_workers=3) as executor:
                future_financial_data = executor.submit(get_financial_data, ticker)
                future_real_time_price = executor.submit(get_real_time_stock_price, ticker)
                future_previous_close_price = executor.submit(get_previous_close_price, ticker)

            financial_data, ticker_valid = future_financial_data.result()

            if not ticker_valid:
                error = "Invalid ticker symbol. Please enter it again."
            elif financial_data is None:
                error = "Unable to fetch financial data. Please try again later."
            else:
                data = financial_data
                real_time_price = future_real_time_price.result()
                previous_close_price = future_previous_close_price.result()
                if real_time_price is not None and previous_close_price is not None:
                    price_diff = real_time_price - previous_close_price
                    price_diff_percent = (price_diff / previous_close_price) * 100
                    data['RealTimePrice'] = {
                        'value': real_time_price,
                        'diff': price_diff,
                        'diff_percent': price_diff_percent,
                        'color': 'green' if price_diff >= 0 else 'red'
                    }
                else:
                    error = "Unable to fetch real-time stock price. Please try again later."
                core = info_core(ticker)
                core_empty = core.empty

    start_date = '1980-01-01'
    end_date = time.strftime("%Y-%m-%d", time.localtime(time.time()))
    economic_index = get_econ_index_data(start_date,end_date)
    econ_index_chart = create_econ_index_chart(economic_index)

    return render_template('index.html', data=data, core=core, core_empty=core_empty, economic_index=economic_index, econ_index_chart=econ_index_chart, error=error)

if __name__ == '__main__':
    app.run(debug=True)


# create a database engine 
engine = create_engine('postgresql://postgres:12345Abcde@localhost:5432/finance_data_5400x')

# create a session factory
Session = sessionmaker(bind=engine)
Base = declarative_base()

class Core(Base):
    __tablename__ = 'core'

    ticker_id = Column(String, primary_key=True)
    Symbol = Column(String, name="Symbol")
    Name = Column(String, name="Name")
    AssetType = Column(String, name="AssetType")
    Description = Column(Text, name="Description")
    Exchange = Column(String, name="Exchange")
    Currency = Column(String, name="Currency")
    Country = Column(String, name="Country")
    Sector = Column(String, name="Sector")
    Industry = Column(String, name="Industry")
    Address = Column(String, name="Address")
    FiscalYearEnd = Column(String, name="FiscalYearEnd")
    LatestQuarter = Column(String, name="LatestQuarter")
    MarketCapitalization = Column(BigInteger, name="MarketCapitalization")
    EBITDA = Column(BigInteger, name="EBITDA")
    PERatio = Column(Float, name="PERatio")
    PEGRatio = Column(Float, name="PEGRatio")
    BookValue = Column(Float, name="BookValue")
    DividendPerShare = Column(Float, name="DividendPerShare")
    DividendYield = Column(Float, name="DividendYield")
    EPS = Column(Float, name="EPS")
    Beta = Column(Float, name="Beta")
    Week52High = Column(Float, name="Week52High")
    Week52Low = Column(Float, name="Week52Low")
    Day50MovingAverage = Column(Float, name="Day50MovingAverage")
    Day200MovingAverage = Column(Float, name="Day200MovingAverage")
    SharesOutstanding = Column(BigInteger, name="SharesOutstanding")
    
class TickerInfo(Base):
    __tablename__ = 'ticker_info'

    ticker_id = Column(Integer, ForeignKey('core.ticker_id'), primary_key=True)
    Symbol = Column(String)
    Date = Column(TIMESTAMP)
    CIK = Column(Integer)
    url_10k = Column(Text)
    logo_url = Column(Text)

    core = relationship("Core", back_populates="ticker_info")
    
class TickerData(Base):
    __tablename__ = 'ticker_data'

    ticker_id = Column(Integer, ForeignKey('core.ticker_id'), primary_key=True)
    Symbol = Column(String)
    Date = Column(TIMESTAMP)
    Year_mon = Column(String)
    Close = Column(Float)
    High = Column(Float)
    Low = Column(Float)
    Open = Column(Float)
    S = Column(String)
    Volume = Column(Float)

    core = relationship("Core", back_populates="ticker_data")
    
Core.ticker_info = relationship("TickerInfo", order_by=TickerInfo.ticker_id, back_populates="core")
Core.ticker_data = relationship("TickerData", order_by=TickerData.ticker_id, back_populates="core")

Base.metadata.create_all(engine)

#---------------------------------------------------------------------------------------------------------------------------------------------

def get_info_core(ticker):
    df = comp_info(ticker)
    
    core = {
        'Symbol' : ticker,
        'Name': df.loc['Name']['Value'],
        'AssetType': df.loc['AssetType']['Value'],
        'Description': df.loc['Description']['Value'],
        'Exchange': df.loc['Exchange']['Value'],
        'Currency': df.loc['Currency']['Value'],
        'Country': df.loc['Country']['Value'],
        'Sector': df.loc['Sector']['Value'],
        'Industry': df.loc['Industry']['Value'],
        'Address': df.loc['Address']['Value'],
        'FiscalYearEnd': df.loc['FiscalYearEnd']['Value'],
        'LatestQuarter': df.loc['LatestQuarter']['Value'],
        'MarketCapitalization': df.loc['MarketCapitalization']['Value'],
        'EBITDA': df.loc['EBITDA']['Value'],
        'PERatio': df.loc['PERatio']['Value'],
        'PEGRatio': df.loc['PEGRatio']['Value'],
        'BookValue': df.loc['BookValue']['Value'],
        'DividendPerShare': df.loc['DividendPerShare']['Value'],
        'DividendYield': df.loc['DividendYield']['Value'],
        'EPS': df.loc['EPS']['Value'],
        'Beta': df.loc['Beta']['Value'],
        'Week52High': df.loc['52WeekHigh']['Value'],  # Updated key name
        'Week52Low': df.loc['52WeekLow']['Value'],    # Updated key name
        'Day50MovingAverage': df.loc['50DayMovingAverage']['Value'],
        'Day200MovingAverage': df.loc['200DayMovingAverage']['Value'],
        'SharesOutstanding': df.loc['SharesOutstanding']['Value']
    }

    return(core)

def get_ticker_info(ticker):
    cik = get_cik_for_ticker(ticker)
    if cik is None:
        print(f"Wrong ticker! {ticker}")
        return None, False

    url_10k = get_most_recent_10k_url(cik, ticker)
    logo_url = f'https://universal.hellopublic.com/companyLogos/{ticker}@2x.png'

    if url_10k is not None:
        return {'Symbol': ticker, 'CIK': cik, '10-K_URL': url_10k, 'Logo_URL': logo_url}, True
    else:
        return None, True
    
def get_ticker_data(ticker):
    now = time.strftime("%Y-%m-%d", time.localtime(time.time()))
    start_date = int(datetime.strptime('1980-01-01', '%Y-%m-%d').timestamp())
    end_date = int(datetime.strptime(now, '%Y-%m-%d').timestamp())

    url = f'https://finnhub.io/api/v1/stock/candle?symbol={ticker}&resolution=D&from={start_date}&to={end_date}&token={API_KEY}'
    
    try:
        response = requests.get(url)
        data = response.json()
    except Exception as e:
        print(f"Error fetching data for symbol {ticker}: {e}")
        return None

    if 't' in data and 'o' in data and 'h' in data and 'l' in data and 'c' in data and 'v' in data:
        df = pd.DataFrame(data)
        df['t'] = pd.to_datetime(df['t'], unit='s')
        df['Symbol'] = ticker
        df.rename(columns={'t': 'Date', 'o': 'Open', 'h': 'High', 'l': 'Low', 'c': 'Close', 'v': 'Volume'}, inplace=True)
        return df
    else:
        return None

#---------------------------------------------------------------------------------------------------------------------------------------------

def get_financial_data(ticker):
    # Retrieve data from API
    core_data = get_info_core(ticker)
    #core_data['Symbol'] = ticker
    ticker_info_data = get_ticker_info(ticker)
    ticker_history_data = get_ticker_data(ticker)

    # Add data to the database
    session = Session()
    try:
        # Add core_data
        core = Core(**core_data)
        session.add(core)
        session.flush()  # To get the ticker_id for foreign key relationships

        # Add ticker_info_data
        ticker_info_data['ticker_id'] = core.ticker_id
        ticker_info = TickerInfo(**ticker_info_data)
        session.add(ticker_info)

        # Add ticker_history_data
        for year_month, history in ticker_history_data.items():
            history_data = {
                'ticker_id': core.ticker_id,
                'Symbol': ticker_symbol,
                'Date': history['Date'],
                'Year_mon': year_month,
                'Close': history['Close'],
                'High': history['High'],
                'Low': history['Low'],
                'Open': history['Open'],
                'S': history['S'],
                'Volume': history['Volume']
            }
            ticker_data = TickerData(**history_data)
            session.add(ticker_data)

        session.commit()
    except SQLAlchemyError as e:
        print(f"Error: {e}")
        session.rollback()
    finally:
        session.close()

# Example usage:
get_financial_data('AAPL')

