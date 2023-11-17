import streamlit as st
from company import company_page
from macroeconomics import macroeconomics_page

def main_page():
    st.image("Finner.png")
    st.subheader(The Ultimate Financial Data Dashboard")
    st.write("created by Xenos Hu.")

def main():
    st.sidebar.title("Navigation")
    page = st.sidebar.selectbox("", 
                                ["Main Page", "Company Information", "Macroeconomic Indicators"])

    if page == "Main Page":
        main_page()
    elif page == "Company Information":
        company_page()
    elif page == "Macroeconomic Indicators":
        macroeconomics_page()

if __name__ == "__main__":
    main()
