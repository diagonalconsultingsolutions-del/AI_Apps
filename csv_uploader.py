import streamlit as st
from snowflake.snowpark.context import get_active_session
import pandas as pd
import json
import os
import io
import re
import time

# -------------------------------
# Setup Snowflake session
# -------------------------------
session = get_active_session()

def clean_name(s):
    s = re.sub(r"[^\w]+", "_", s)       # replace non-alphanum with underscore
    return re.sub(r'_+', '_', s).strip('_').upper()


def process_df(df):
    df.columns = [clean_name(c) for c in df.columns]
    #df.to_csv(path, index=False)
    return df


    
# -------------------------------------
# csv Upload Streamlit app
# -------------------------------------
def csv_uploader():

    # -------------------------
    # Stage settings
    # -------------------------

    stage_name_no_at = "core"
    
    stage_name = f"@{stage_name_no_at}"

    
    # -------------------------
    # File upload tab
    # -------------------------
    st.header("File Upload")
    st.write(f"Upload files to Snowflake App stage {stage_name_no_at}")

    uploaded_file = st.file_uploader("Choose a file")

    if uploaded_file:
        with st.spinner("Staging..."): 
            file_extension = os.path.splitext(uploaded_file.name)[1].lower()
            try:
                # Create file stream using BytesIO and upload
                file_stream = io.BytesIO(uploaded_file.getvalue())
                session.file.put_stream(
                    file_stream,
                    f"{stage_name}/{uploaded_file.name}",
                    auto_compress=False,
                    overwrite=True
                )
                st.success(f"File '{uploaded_file.name}' staging Successful")
    
                try:
                    uploaded_file_pd = pd.read_csv(uploaded_file)
    
                except Exception as e:
                    st.warning(f"Error occurred while reading the file: {str(e)}")
            except Exception as e:
                st.error(f"Error occurred while uploading file: {str(e)}")
        with st.spinner("Cleaning data and creating table..."):
            try:
                clean_upload_pd = process_df(uploaded_file_pd)
                st.success("Data cleaning Successful")
                table_cols = ",\n    ".join([f'"{c}" VARCHAR' for c in clean_upload_pd.columns])
                create_table_sql = f"CREATE OR REPLACE TABLE INPUT_AI_CLASSIFY_APP (\n    {table_cols}\n);"
                st.success("CREATE TABLE Successful")
            except Exception as e:
                st.code(create_table_sql)
                st.error(f"Failed to write to table: : {str(e)}")
        with st.spinner("Copying data into table..."):
            try:
                copy_into_sql = f"""COPY INTO INPUT_AI_CLASSIFY_APP
                FROM @{stage_name_no_at}/{uploaded_file.name}
                FILE_FORMAT = (FORMAT_NAME = CSV_FORMAT)
                ON_ERROR = 'CONTINUE';"""
                session.sql(copy_into_sql).collect()
                preview_upload = session.sql("SELECT * FROM INPUT_AI_CLASSIFY_APP").to_pandas()
                st.success("COPY INTO Successful")
                st.markdown("### SQL CREATE TABLE EXECUTED:")
                st.code(create_table_sql)
                st.markdown("### SQL COPY INTO EXECUTED:")
                st.code(copy_into_sql)
                st.markdown("### DATA Preview:")
                st.dataframe(preview_upload.head())
            except Exception as e:
                st.code(copy_into_sql)
                st.error(f"Failed to copy into table: : {str(e)}")

   
# -------------------------------------
# Launch CSV Uploader
# -------------------------------------
if __name__ == "__main__":
    csv_uploader() 

