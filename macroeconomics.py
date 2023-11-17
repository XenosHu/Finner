#!/usr/bin/env python
# coding: utf-8

def macroeconomics_page():
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

    st.title("Macroeconomic Indicators")
    
    df = pd.DataFrame()
    
    def get_econ_index_data(start_date,end_date):
    
        start = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')
    
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
    
    # Function to create economic index chart
    def create_econ_index_chart(df):
        fig = px.line(df, 
                      x=df.index, 
                      y=['NASDAQ Composite Index', 'CPI', 'PPI', 'M1', 'M2', 'Unemployment Rate %', 'Real GDP', 'Real GDP Per Capita', 'FED Effective Rate', 'PCE', 'U.S. Dollar Index', 'Median Household Income'],
                      title='Economic Index',
                      labels={'value': 'Value', 'index': 'Date'},
                      width=800, height=600)
        
        st.plotly_chart(fig)

    # Displaying economic index chart
    st.write("Economic Index Chart:")
    economic_index = get_econ_index_data('1980-01-01', time.strftime("%Y-%m-%d", time.localtime(time.time())))
    create_econ_index_chart(economic_index)


