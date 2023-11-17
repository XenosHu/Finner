#!/usr/bin/env python
# coding: utf-8

def company_page():
    import asyncio
    import nest_asyncio
    import numpy as np
    import random
    import string
    import os
    import pandas as pd
    import pandas_datareader as pdr
    from concurrent.futures import ThreadPoolExecutor
    from datetime import datetime, timedelta
    from finnhub import Client as FinnhubClient
    from pandas_datareader import DataReader
    from plotly import colors, express as px, graph_objects as go, offline as pyo
    from requests.exceptions import RequestException
    import requests
    import streamlit as st
    import time

    
    # # 1. Set-up the environment
    ticker_name = pd.DataFrame(pd.read_csv("nasdaq-listed.csv"))
    
    nest_asyncio.apply()  
    
    # API_KEY = 'cgn4mghr01qhveut0q00cgn4mghr01qhveut0q0g'
    API_KEY = 'ch4vj6pr01quc2n51ud0ch4vj6pr01quc2n51udg'
    
    async def get_data(symbol, start_date, end_date):
        url = f'https://finnhub.io/api/v1/stock/candle?symbol={symbol}&resolution=D&from={start_date}&to={end_date}&token={API_KEY}'
        try:
            response = await loop.run_in_executor(None, requests.get, url)
            data = response.json()
        except Exception as e:
            st.error(f"Error fetching data for symbol {symbol}: {e}")
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
    
    # # 3. EDGAR 10-K Filing and Logo URLs
    
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
            st.write(f"Failed to fetch data for {ticker} after multiple attempts")
            return None
    
        all_forms = pd.DataFrame.from_dict(filing_metadata.json()['filings']['recent'])
    
        most_recent_10k_filtered = all_forms.loc[all_forms['form'] == '10-K']
        
        if most_recent_10k_filtered.empty:
            st.write(f"No 10-K form found for {ticker}")
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
        API_KEY = '9PRUKP0VYXOA80BA'
        url = f'https://www.alphavantage.co/query?function=OVERVIEW&symbol={ticker}&apikey={API_KEY}'
        response = requests.get(url)
        data = response.json()
    
        if 'Name' in data:
            name_value = data['Name']
            print(f"Name: {name_value}")
        else:
            print("Warning: 'Name' not found in response")
        
        df = pd.DataFrame(list(data.items()), columns=['Attribute', 'Value'])
        return df
    
    def info_core(ticker):
        df = comp_info(ticker)
    
        core = pd.DataFrame({
            'Name': df[df['Attribute'] == 'Name']['Value'].iloc[0],
            'Symbol': df[df['Attribute'] == 'Symbol']['Value'].iloc[0],
            'AssetType': df[df['Attribute'] == 'AssetType']['Value'].iloc[0],
            'Description': df[df['Attribute'] == 'Description']['Value'].iloc[0],
            'Exchange': df[df['Attribute'] == 'Exchange']['Value'].iloc[0],
            'Currency': df[df['Attribute'] == 'Currency']['Value'].iloc[0],
            'Country': df[df['Attribute'] == 'Country']['Value'].iloc[0],
            'Sector': df[df['Attribute'] == 'Sector']['Value'].iloc[0],
            'Industry': df[df['Attribute'] == 'Industry']['Value'].iloc[0],
            'Address': df[df['Attribute'] == 'Address']['Value'].iloc[0],
            'FiscalYearEnd': df[df['Attribute'] == 'FiscalYearEnd']['Value'].iloc[0],
            'LatestQuarter': df[df['Attribute'] == 'LatestQuarter']['Value'].iloc[0],
            'MarketCapitalization': df[df['Attribute'] == 'MarketCapitalization']['Value'].iloc[0],
            'EBITDA': df[df['Attribute'] == 'EBITDA']['Value'].iloc[0],
            'PERatio': df[df['Attribute'] == 'PERatio']['Value'].iloc[0],
            'PEGRatio': df[df['Attribute'] == 'PEGRatio']['Value'].iloc[0],
            'BookValue': df[df['Attribute'] == 'BookValue']['Value'].iloc[0],
            'DividendPerShare': df[df['Attribute'] == 'DividendPerShare']['Value'].iloc[0],
            'DividendYield': df[df['Attribute'] == 'DividendYield']['Value'].iloc[0],
            'EPS': df[df['Attribute'] == 'EPS']['Value'].iloc[0],
            'Beta': df[df['Attribute'] == 'Beta']['Value'].iloc[0],
            '52WeekHigh': df[df['Attribute'] == '52WeekHigh']['Value'].iloc[0],
            '52WeekLow': df[df['Attribute'] == '52WeekLow']['Value'].iloc[0],
            '50DayMovingAverage': df[df['Attribute'] == '50DayMovingAverage']['Value'].iloc[0],
            '200DayMovingAverage': df[df['Attribute'] == '200DayMovingAverage']['Value'].iloc[0],
            'SharesOutstanding': df[df['Attribute'] == 'SharesOutstanding']['Value'].iloc[0]
        }, index=[0])
    
        core = core.transpose()
        return core
    
    def info_other(ticker):
        df = comp_info(ticker)
        core = info_core(ticker)
        other = df.loc[~df.index.isin(core.columns)]
        other = other.transpose()
        return(other)
    
    # # 6. Interactive UI
    
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
            st.error(f"Invalid candlestick data format for {symbol}")
            return None
    
        df = pd.DataFrame({'t': data['t'], 'o': data['o'], 'h': data['h'], 'l': data['l'], 'c': data['c']})
        df['t'] = pd.to_datetime(df['t'], unit='s')
        df['Date'] = df['t'].dt.strftime('%Y-%m-%d')
    
        st.write("Candlestick Data:")
        st.write(df)
    
        fig = go.Figure(data=[go.Candlestick(x=df['Date'],
                                             open=df['o'],
                                             high=df['h'],
                                             low=df['l'],
                                             close=df['c'])])
    
        fig.update_layout(title=f"{symbol} Stock Prices",
                          xaxis_title="Date",
                          yaxis_title="Price ($)",
                          xaxis_rangeslider_visible=False)
    
        # Return the Plotly Figure directly
        return fig
    
    # Function to get financial data
    def get_financial_data(ticker):
        cik = get_cik_for_ticker(ticker)
        if cik is None:
            st.error(f"Wrong ticker! {ticker}")
            return None, False
    
        url_10k = get_most_recent_10k_url(cik, ticker)
        logo_url = f'https://universal.hellopublic.com/companyLogos/{ticker}@2x.png'
        candlestick_data = fetch_candlestick_data(ticker)
        chart = create_candlestick_chart(candlestick_data, ticker)
    
        if url_10k is not None and chart is not None:
            return {'Symbol': ticker, 'CIK': cik, '10-K_URL': url_10k, 'Logo_URL': logo_url, 'Chart': chart}, True
        else:
            return None, True
    
    # Function to get previous close price
    def get_previous_close_price(ticker):
        api_key = 'cgn4mghr01qhveut0q00cgn4mghr01qhveut0q0g'
        url = f'https://finnhub.io/api/v1/quote?symbol={ticker}&token={api_key}'
        response = requests.get(url)
        if response.status_code == 200:
            stock_data = response.json()
            return stock_data['pc']
        else:
            return None
    
    # Function to get real-time stock price
    def get_real_time_stock_price(ticker):
        api_key = 'cgn4mghr01qhveut0q00cgn4mghr01qhveut0q0g'
        url = f'https://finnhub.io/api/v1/quote?symbol={ticker}&token={api_key}'
        response = requests.get(url)
        if response.status_code == 200:
            stock_data = response.json()
            return stock_data['c']
        else:
            return None
    
    # def main():
    # Displaying an image with st.image
    image_url = "Finner.png"
    left_co, cent_co, last_co = st.columns(3)

    with cent_co:
        st.image(image_url)

    st.title("Company Information")
    #st.title("Financial Data Dashboard For Nasdaq Tickers")

    ticker = st.text_input("Enter Ticker Symbol:")
    if st.button("Submit"):
        error = None
        data = None
        core = None
        core_empty = True

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

            # Displaying financial data in a structured format
            if data is not None:
                st.write("Financial Data:")
                st.write(f"**Symbol:** {data['Symbol']}")
                st.write(f"**CIK:** {data['CIK']}")
                st.write(f"**10-K URL:** [Link]({data['10-K_URL']})")
            
                # Displaying Logo (resized)
                st.markdown(f'<img src="{data["Logo_URL"]}" alt="Logo for {data["Symbol"]}" style="border-radius:50%;" width=100>', unsafe_allow_html=True)
                
                st.write("**Chart:**")
                candlestick_data = fetch_candlestick_data(ticker)
                candlestick_chart = create_candlestick_chart(candlestick_data, ticker)
                st.plotly_chart(candlestick_chart)

                color = 'green' if data['RealTimePrice']['diff'] >= 0 else 'red'

                # Displaying the information with the specified color

                st.write("**Real-Time Price:**")
                st.write(f"Value: {data['RealTimePrice']['value']}")
                st.write(f"Diff: <span style='color:{color}'>{data['RealTimePrice']['diff']:.2f}</span>", unsafe_allow_html=True)
                st.write(f"Diff Percent: <span style='color:{color}'>{data['RealTimePrice']['diff_percent']:.2f}%</span>", unsafe_allow_html=True)

        # Displaying core information
        if not core_empty:
            st.write("Core Information:")
            st.write(core)

