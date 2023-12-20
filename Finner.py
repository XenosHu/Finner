__import__('pysqlite3')
import sys
sys.modules['sqlite3'] = sys.modules.pop('pysqlite3')

import sqlite3
import streamlit as st
from company import company_page
from macroeconomics import macroeconomics_page
from analysis import analysis_page



def main_page():
    st.image("Finner.png")
    st.subheader("The Ultimate Financial Data Dashboard")
    st.write("created by Xenos Hu.")

def main():
    st.sidebar.title("Navigation")
    page = st.sidebar.selectbox("", 
                                ["Main Page", "Company Information", "Macroeconomic Indicators", "Financial Report Analyzer"])

    if page == "Main Page":
        main_page()
    elif page == "Company Information":
        company_page()
    elif page == "Macroeconomic Indicators":
        macroeconomics_page()
    elif page == "Financial Report Analyzer":
        analysis_page()



if __name__ == "__main__":
    main()
