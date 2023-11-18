def analysis_page():
    import os
    import tempfile
    from langchain.llms import OpenAI
    from langchain.embeddings import OpenAIEmbeddings
    import streamlit as st
    from langchain.document_loaders import PyPDFLoader, HTMLLoader  # Assuming HTMLLoader is available
    from langchain.vectorstores import Chroma
    from langchain.agents.agent_toolkits import (
        create_vectorstore_agent,
        VectorStoreToolkit,
        VectorStoreInfo
    )

    st.title('GPT Financial Report Analyzer')

    # User inputs their OpenAI API key
    api_key = st.text_input("Enter your OpenAI API key", type="password")

    if api_key:
        # Set the API key environment variable
        os.environ['OPENAI_API_KEY'] = api_key

        # Create instance of OpenAI LLM
        llm = OpenAI(temperature=0.1, verbose=True)
        embeddings = OpenAIEmbeddings()

        # Upload file
        uploaded_file = st.file_uploader("Upload your document (PDF or HTML)", type=["pdf", "html", "htm"])
        if uploaded_file:
            # Determine file type and process accordingly
            file_type = uploaded_file.type
            suffix = ".pdf" if file_type == "application/pdf" else ".html"
            loader_class = PyPDFLoader if file_type == "application/pdf" else HTMLLoader

            # Save the uploaded file to a temporary file
            with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp_file:
                tmp_file.write(uploaded_file.read())
                temp_file_path = tmp_file.name

            # Load document using appropriate loader
            loader = loader_class(temp_file_path)
            pages = loader.load_and_split()

            # Load documents into ChromaDB
            store = Chroma.from_documents(pages, embeddings, collection_name='annualreport')

            # Create vectorstore info object
            vectorstore_info = VectorStoreInfo(
                name="annual_report",
                description="A document uploaded by the user",
                vectorstore=store
            )

            # Convert the document store into a langchain toolkit
            toolkit = VectorStoreToolkit(vectorstore_info=vectorstore_info)

            # Add the toolkit to an end-to-end LC
            agent_executor = create_vectorstore_agent(
                llm=llm,
                toolkit=toolkit,
                verbose=True
            )

            # Create a text input box for the user
            prompt = st.text_input('Input your prompt here')

            # If the user inputs a prompt
            if prompt:
                # Then pass the prompt to the LLM
                response = agent_executor.run(prompt)
                # ...and write it out to the screen
                st.write(response)

                # With a streamlit expander  
                with st.expander('Document Similarity Search'):
                    # Find the relevant pages
                    search = store.similarity_search_with_score(prompt) 
                    # Write out the first 
                    st.write(search[0][0].page_content)
