import streamlit as st
from snowflake.snowpark.context import get_active_session
import pandas as pd
import json
import re


# -------------------------------
# Setup Snowflake session
# -------------------------------
session = get_active_session()

st.title("Snowflake Cortex Complete")

def q(name):
    return f'"{name}"'

# -------------------------------
# Step 1-3: Database / Schema / Stage / File
# -------------------------------



databases = [row["name"] for row in session.sql("SHOW DATABASES").collect()]

database = st.selectbox("Select Database", databases)

schemas = [row["name"] for row in session.sql(f"SHOW SCHEMAS IN {q(database)}").collect()]
schema = st.selectbox("Select Schema", schemas)

stages = [row["name"] for row in session.sql(f"SHOW STAGES IN {q(database)}.{q(schema)}").collect()]
stage = st.selectbox("Select Stage", stages)

files = [row["name"] for row in session.sql(f"ls @{q(database)}.{q(schema)}.{q(stage)}").collect()]
file = st.selectbox("Select pdf File", files)

full_filepath = f"{q(database)}.{q(schema)}.{q(file)}"

safe_filename = re.sub(r'[^A-Za-z0-9_]', '_', file).upper()
parsed_table = f'PARSED_{safe_filename}'
full_tablename = f'{database}.{schema}.{parsed_table}'

parted_filename = re.findall(r'[^/]+', file)
clean_filename = parted_filename[-1]
#st.markdown(clean_filename)


# -------------------------------
# Step 4: Parse Text
# -------------------------------

modes = ['OCR', 'LAYOUT']
mode = st.selectbox("Select Mode", modes)


splits = ['FALSE', 'TRUE']
split = st.selectbox("Select Page Split", splits)



#'page_filter': [{'start': 0, 'end': 1}]}
options = {'mode': mode, 'page_split': split}



sql = f"""
CREATE OR REPLACE TABLE {full_tablename} AS
SELECT
    *,
        SNOWFLAKE.CORTEX.PARSE_DOCUMENT(
            '@{database}.{schema}.{stage}',
            '{clean_filename}',
            {options}
            ) AS parsed_text
FROM DIRECTORY(@{database}.{schema}.{stage})
WHERE RELATIVE_PATH = '{clean_filename}'
"""

st.subheader("Parse Document")
st.code(sql)
if st.button("Parse Document"):
    with st.spinner("Parsing Document"):
        session.sql(sql).collect()
        st.success("PDF parsed and table created")
    
        df = session.sql(f"SELECT * FROM {full_tablename}").to_pandas()
        st.dataframe(df)
        st.session_state.parsed_text = df.iloc[0, 6]

if 'parsed_text' in st.session_state:

# -------------------------------
# Step 5: AI_COMPLETE Parameter Builder
# -------------------------------
    core_models = [
        "claude-4-opus",
        "claude-4-sonnet",
        "claude-3-7-sonnet",
        "claude-3-5-sonnet",
        "deepseek-r1",
        "llama3-8b",
        "llama3-70b",
        "llama3.1-8b",
        "llama3.1-70b",
        "llama3.1-405b",
        "llama3.3-70b",
        "llama4-maverick",
        "llama4-scout",
        "mistral-large",
        "mistral-large2",
        "mistral-7b",
        "mixtral-8x7b",
        "openai-gpt-4.1",
        "openai-o4-mini",
        "snowflake-arctic",
        "snowflake-llama-3.1-405b",
        "snowflake-llama-3.3-70b"
    ]
    
    finetuned_models = [row["name"] for row in session.sql(f"SHOW MODELS IN ACCOUNT").collect()]
    all_models = core_models + finetuned_models
    model = st.selectbox("Select LLM Model", core_models)
    
    # system_prompt = st.text_input("System Prompt",
    #           placeholder="Background information and instructions for a response style..."
    #       )
    
    # user_prompt = st.text_input("User Prompt",
    #           placeholder="A prompt provided by the user..."
    #       )
    
    prompt = st.text_input("Prompt",
             placeholder="A prompt provided by the user..."
         )
    
    # prompt_dev = [
    #     {'role': 'system', 'content': system_prompt},
    #     {'role': 'user', 'content': user_prompt}
    #  ]
    
    #full_prompt_dev = f"{prompt} {st.session_state.parsed_text}"     #Makes a string for input into the sql
    
    
    def sql_escape(s):
        return s.replace("'", "''")
    
    full_prompt_dev = sql_escape(
        f"{prompt}\n\nDOCUMENT CONTENT:\n{st.session_state.parsed_text}"
    )
    
    st.markdown(type(full_prompt_dev))
    
    temperature = st.slider("Temperature (Increases the randomness of the output of the language model.)", min_value=0, max_value=10, value=0, step=1)
    temperature = temperature / 10
    
    top_p = st.slider("Top_p (Restricts the set of possible tokens that the model outputs)", min_value=0, max_value=10, value=0, step=1)
    top_p = top_p / 10
    
    max_tokens = st.slider("max_tokens (Sets the maximum number of output tokens in the response. Small values can result in truncated responses. Default: 4096 Maximum allowed value: 8192)", min_value=0, max_value=8192, value=4096, step=512)
    max_tokens = float(max_tokens)
    
    response_type = 'json'
    response_format = {'type': response_type}
    # -------------------------------
    # # Step 6: AI_COMPLETE CALL
    # -------------------------------
    
    
    sql = f"""
        SELECT AI_COMPLETE(
            '{model}',
            '{full_prompt_dev}',
            OBJECT_CONSTRUCT(
                'temperature', {temperature},
                'top_p', {top_p},
                'max_tokens', {max_tokens}
                                )
            ) AS LLM_SCORE
            
        """
    st.code(sql)
    if st.button("Run Cortex Complete"):
        with st.spinner("Running Cortex Complete"):
            res = session.sql(sql).to_pandas()
            st.dataframe(res)
            cell_value = res["LLM_SCORE"].iloc[0]  # row 0
            st.markdown("### Response")
            st.markdown(cell_value)
    
    


