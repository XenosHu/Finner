def analysis_page():
    import os
    import tempfile
    from datetime import datetime
    import requests
    from langchain.llms import OpenAI
    from langchain.embeddings import OpenAIEmbeddings
    import streamlit as st
    import pdfkit
    from langchain.document_loaders import PyPDFLoader  # Assuming HTMLLoader is available
    from langchain.vectorstores import Chroma
    from langchain.agents.agent_toolkits import (
        create_vectorstore_agent,
        VectorStoreToolkit,
        VectorStoreInfo
    )

    def download_pdf(ticker, year):
        url = f"https://www.annualreports.com/HostedData/AnnualReports/PDF/NASDAQ_{ticker}_{year}.pdf"
        response = requests.get(url)
        if response.status_code == 200:
            return response.content
        return None
    
    st.title('GPT Financial Report Analyzer')

    # User inputs their OpenAI API key
    api_key = st.text_input("Enter your OpenAI API key", type="password")

    if api_key:
        # Set the API key environment variable
        os.environ['OPENAI_API_KEY'] = api_key

        # Create instance of OpenAI LLM
        llm = OpenAI(temperature=0.1, verbose=True)
        embeddings = OpenAIEmbeddings()
        
        ticker_input = st.text_input("Enter Ticker Symbol:")
        uploaded_file = st.file_uploader("Or upload a PDF document", type="pdf")

        if ticker_input or uploaded_file:
            current_year = datetime.now().year
            pdf_content = None

            if ticker_input:
                for year in range(current_year, current_year - 5, -1):  # Try the last five years
                    pdf_content = download_pdf(ticker_input, year)
                    if pdf_content:
                        break

                if not pdf_content:
                    st.error("No report found for the given ticker in the last 5 years.")
                    return

            if uploaded_file:
                pdf_content = uploaded_file.read()

            with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp_file:
                tmp_file.write(pdf_content)
                temp_file_path = tmp_file.name

            loader = PyPDFLoader(temp_file_path)
            pages = loader.load_and_split()

            store = Chroma.from_documents(pages, embeddings, collection_name='uploaded_document')
    
            
            # uploaded_file = st.file_uploader("Upload your document (PDF or HTML)", type=["pdf", "html", "htm"])
            # if uploaded_file:
            #     file_type = uploaded_file.type
            #     suffix = ".pdf"
    
            #     with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp_file:
            #         if file_type == "application/pdf":
            #             tmp_file.write(uploaded_file.read())
            #         else:
            #             # Convert HTML to PDF
            #             pdfkit.from_file(uploaded_file, tmp_file.name)
                    
            #         temp_file_path = tmp_file.name
    
            #     # Load and process the PDF file
            #     loader = PyPDFLoader(temp_file_path)
            #     pages = loader.load_and_split()
    
            #     store = Chroma.from_documents(pages, embeddings, collection_name='uploaded_document')
    
            vectorstore_info = VectorStoreInfo(
                name="uploaded_document",
                description="A document uploaded by the user",
                vectorstore=store
            )

            toolkit = VectorStoreToolkit(vectorstore_info=vectorstore_info)
            agent_executor = create_vectorstore_agent(llm=llm, toolkit=toolkit, verbose=True)

            prompt = st.text_input('Input your prompt here')
            if prompt:
                response = agent_executor.run(prompt)
                st.write(response)

                with st.expander('Document Similarity Search'):
                    search = store.similarity_search_with_score(prompt)
                    st.write(search[0][0].page_content)

        
        # uploaded_file = st.file_uploader("Upload your annual report PDF", type="pdf")
        # if uploaded_file:
        #     # Save the uploaded file to a temporary file
        #     with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp_file:
        #         tmp_file.write(uploaded_file.read())
        #         temp_file_path = tmp_file.name
    
        #     # Now use this temporary file path with PyPDFLoader
        #     loader = PyPDFLoader(temp_file_path)
        #     # Load documents into ChromaDB
        #     store = Chroma.from_documents(pages, embeddings, collection_name='annualreport')
    
        #     # Create vectorstore info object
        #     vectorstore_info = VectorStoreInfo(
        #         name="annual_report",
        #         description="A document uploaded by the user",
        #         vectorstore=store
        #     )
    
        #     # Convert the document store into a langchain toolkit
        #     toolkit = VectorStoreToolkit(vectorstore_info=vectorstore_info)
    
        #     # Add the toolkit to an end-to-end LC
        #     agent_executor = create_vectorstore_agent(
        #         llm=llm,
        #         toolkit=toolkit,
        #         verbose=True
        #     )
    
        #     # Create a text input box for the user
        #     prompt = st.text_input('Input your prompt here')
    
        #     # If the user inputs a prompt
        #     if prompt:
        #         # Then pass the prompt to the LLM
        #         response = agent_executor.run(prompt)
        #         # ...and write it out to the screen
        #         st.write(response)
    
        #         # With a streamlit expander  
        #         with st.expander('Document Similarity Search'):
        #             # Find the relevant pages
        #             search = store.similarity_search_with_score(prompt) 
        #             # Write out the first 
        #             st.write(search[0][0].page_content)
