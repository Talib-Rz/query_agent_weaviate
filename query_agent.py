
import os
import streamlit as st
import pandas as pd
from tempfile import TemporaryDirectory
from weaviate import connect_to_weaviate_cloud
from weaviate.auth import Auth
from weaviate.classes.config import Configure, Property, DataType
from weaviate.agents.query import QueryAgent
from weaviate.agents.utils import print_query_agent_response
import re
from dotenv import load_dotenv
import atexit


# Load environment variables
load_dotenv()
WEAVIATE_URL = os.getenv("WEAVIATE_URL")
WEAVIATE_API_KEY = os.getenv("WEAVIATE_API_KEY")

# Reserved field names
RESERVED_NAMES = {"id"}

# Connect to Weaviate Cloud
client = connect_to_weaviate_cloud(
    cluster_url=WEAVIATE_URL,
    auth_credentials=Auth.api_key(WEAVIATE_API_KEY)
)

def delete_existing_collections():
    try:
        existing = client.collections.list_all()
        for collection_name in existing:
            client.collections.delete(collection_name)
        st.info(" Cleared all existing collections in Weaviate.")
    except Exception as e:
        st.error(f" Failed to delete collections: {e}")

# Utility: Clean property names
def clean_property_name(name: str) -> str:
    name = name.strip().lower().replace(" ", "_")
    name = re.sub(r"[^a-zA-Z0-9_]", "_", name)
    if not re.match(r"^[a-zA-Z_]", name):
        name = "_" + name
    return name

# Upload and prepare data
def process_uploaded_files(uploaded_files):
    collections_created = []
    table_schemas = []
    
    for file in uploaded_files:
        filename = file.name
        table_name = os.path.splitext(filename)[0].replace(" ", "_").lower()

        try:
            df = pd.read_excel(file) if filename.endswith(".xlsx") else pd.read_csv(file)
        except Exception as e:
            st.error(f"Failed to read file: {filename}, error: {e}")
            continue

        df = df.dropna(axis=1, how='all')
        rename_map = {}
        props = []
        schema_desc = f"Table: {table_name}\n"

        for col in df.columns:
            col_clean = clean_property_name(col)
            if col_clean in RESERVED_NAMES:
                col_clean += "_field"
            rename_map[col] = col_clean

            dtype_enum = DataType.NUMBER if pd.api.types.is_numeric_dtype(df[col]) else DataType.TEXT
            props.append(Property(name=col_clean, data_type=dtype_enum))
            schema_desc += f"- {col_clean}: {'Number' if dtype_enum == DataType.NUMBER else 'Text'}\n"

        df.rename(columns=rename_map, inplace=True)

        # Delete and recreate collection
        if client.collections.exists(table_name):
            client.collections.delete(table_name)
        client.collections.create(table_name, vectorizer_config=Configure.Vectorizer.text2vec_weaviate(), properties=props)

        # Upload data
        collection = client.collections.get(table_name)
        with collection.batch.dynamic() as batch:
            for _, row in df.iterrows():
                batch.add_object(properties=row.dropna().to_dict())

        table_schemas.append(schema_desc)
        collections_created.append(table_name)
        st.success(f"Uploaded and created collection: {table_name}")
    
    return collections_created, "\n\n".join(table_schemas)


# UI Layout

# === Title and Description ===
st.title("Excel Query Agent (Weaviate)")
st.markdown("Upload Excel files and ask questions using natural language.")

delete_existing_collections()

# === File Upload ===
uploaded_files = st.file_uploader("Upload one or more Excel or CSV files", type=["xlsx", "csv"], accept_multiple_files=True)

# === Define your close_connection function ===
def close_connection():
    if "client" in st.session_state:
        try:
            st.session_state.client.close()
            print("Client connection closed.")
        except Exception as e:
            print(f"Error closing client: {e}")

# Register cleanup function only once
if "cleanup_registered" not in st.session_state:
    atexit.register(close_connection)
    st.session_state.cleanup_registered = True

# === File Processing & Agent Setup ===
if uploaded_files:
    collections, schema_prompt = process_uploaded_files(uploaded_files)

    role_prompt = (
        "You are a Project Manager analyzing site rollout readiness. "
        "You use structured datasets and apply filters and logic as described in the question.\n\n"
        "Below is the schema of the tables uploaded by the user:\n\n"
        f"{schema_prompt}\n"
        "Always reason step-by-step and provide clear, business-ready answers."
    )

    # Initialize client if needed (define your client logic)
    if "client" not in st.session_state:
            st.session_state.client = client  # <-- Define this function as needed

    # Initialize the agent only once
    if "query_agent" not in st.session_state:
        st.session_state.query_agent = QueryAgent(
            client=st.session_state.client,
            collections=collections,
            system_prompt=role_prompt
        )

    st.success("Query agent is ready.")

    # === Query Interface ===
    query = st.text_input("Ask a question about your uploaded data:")
    if query and st.session_state.query_agent:
        response = st.session_state.query_agent.run(query)
        st.subheader("Query Response")
        st.write(response.final_answer)
        with st.expander("Intermediate Info"):
            st.json(response.model_dump())
















#================ without streamlit ========================



# import os
# import pandas as pd
# from docx import Document
# from dotenv import load_dotenv
# from weaviate import connect_to_weaviate_cloud
# from weaviate.auth import Auth
# from weaviate.classes.config import Configure, Property, DataType
# from weaviate.agents.query import QueryAgent
# from weaviate.agents.utils import print_query_agent_response

# # Load env variables
# load_dotenv()
# WEAVIATE_URL = os.getenv("WEAVIATE_URL")
# WEAVIATE_API_KEY = os.getenv("WEAVIATE_API_KEY")

# # Connect to Weaviate Cloud
# client = connect_to_weaviate_cloud(
#     cluster_url=WEAVIATE_URL,
#     auth_credentials=Auth.api_key(WEAVIATE_API_KEY)
# )

# # Reserved Weaviate property names
# RESERVED_NAMES = {"id"}

# def parse_input_data(input_dir):
#     excel_dataframes = {}
#     guideline_text = ""

#     for file in os.listdir(input_dir):
#         if file.endswith('.xlsx') or file.endswith('.csv'):
#             df = pd.read_excel(os.path.join(input_dir, file)) if file.endswith('.xlsx') else pd.read_csv(os.path.join(input_dir, file))
#             table_name = os.path.splitext(file)[0].replace(" ", "_").lower()
#             excel_dataframes[table_name] = df

#     for file in os.listdir(input_dir):
#         if file.endswith('.docx'):
#             doc = Document(os.path.join(input_dir, file))
#             guideline_text = "\n".join([para.text for para in doc.paragraphs if para.text.strip()])

#     return excel_dataframes, guideline_text

# import re

# def clean_property_name(name: str) -> str:
#     # Convert to lowercase and replace invalid characters
#     name = name.strip().lower().replace(" ", "_")
#     name = re.sub(r"[^a-zA-Z0-9_]", "_", name)  # Replace all non-alphanum/underscore with _
#     if not re.match(r"^[a-zA-Z_]", name):
#         name = "_" + name  # Prefix with underscore if name doesn't start with letter/_ 
#     return name

# def setup_weaviate_collections(dataframes):
#     for name, df in dataframes.items():
#         df = df.dropna(axis=1, how='all')
#         columns = df.columns

#         props = []
#         rename_map = {}

#         for col in columns:
#             col_clean = clean_property_name(col)
#             if col_clean in RESERVED_NAMES:
#                 col_clean += "_field"
#                 print(f"[INFO] Renamed reserved column '{col}' to '{col_clean}'")
#             elif col_clean != col:
#                 print(f"[INFO] Renamed invalid column '{col}' to '{col_clean}'")
#             rename_map[col] = col_clean

#             dtype = df[col].dtype
#             dtype_enum = DataType.NUMBER if pd.api.types.is_numeric_dtype(dtype) else DataType.TEXT
#             props.append(Property(name=col_clean, data_type=dtype_enum))

#         # Rename columns in DataFrame
#         df = df.rename(columns=rename_map)

#         # Delete existing collection if needed
#         if client.collections.exists(name):
#             client.collections.delete(name)

#         # Create collection
#         client.collections.create(
#             name,
#             vectorizer_config=Configure.Vectorizer.text2vec_weaviate(),
#             properties=props
#         )

#         # Upload records
#         collection = client.collections.get(name)
#         with collection.batch.dynamic() as batch:
#             for _, row in df.iterrows():
#                 batch.add_object(properties=row.dropna().to_dict())


# def create_query_agent(client, collection_names, system_prompt=None):
#     return QueryAgent(client=client, collections=collection_names, system_prompt=system_prompt)

# def ask_question(agent, question, context=None):
#     response = agent.run(question, context=context)
#     print_query_agent_response(response)
#     return response

# # === MAIN ===
# if __name__ == "__main__":
#     input_dir = "D:/developer_zone/gen_ai/nokia/nokia_part2/Coding/dataset"  # <-- Replace with your folder path

#     dataframes, guideline_text = parse_input_data(input_dir)
#     setup_weaviate_collections(dataframes)

#     agent = create_query_agent(client, list(dataframes.keys()))
#     response = ask_question(agent, "Calculate how many sites are present with alarm code 1000")
#     # ask_question(agent, "Which of them have material available?", context=response)

#     # Close connection cleanly
#     client.close()
