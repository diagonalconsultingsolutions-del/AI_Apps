import os
import io
import pandas as pd
import streamlit as st
from snowflake.snowpark.context import get_active_session

# -------------------------------------
# Get Snowflake session
# -------------------------------------
session = get_active_session()
sessh = session.sql("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA()").collect()
st.markdown(sessh)
# -------------------------------------
# Constants and settings
# -------------------------------------
# File extensions that can be previewed
PREVIEWABLE_EXTENSIONS = ['.csv', '.txt', '.tsv']

# -------------------------------------
# Stage existence check and creation
# -------------------------------------
def ensure_stage_exists(stage_name_no_at: str):
    """
    Creates a stage if it doesn't exist. Does nothing if it already exists.
    """
    # try:
    #     # Check if stage exists
    #     session.sql(f"DESC STAGE {stage_name_no_at}").collect()
    # except:
        # Create stage if it doesn't exist
    try:
        session.sql(f"""
            CREATE OR REPLACE STAGE {stage_name_no_at}
            ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')
        """).collect()
        st.sidebar.success(f"Stage @{stage_name_no_at} has been created.")
    except Exception as e:
        st.sidebar.error(f"Failed to create stage: {str(e)}")
        st.stop()

# -------------------------------------
# Main Streamlit app
# -------------------------------------
def main():
    st.title("Snowflake File Management App")

    # -------------------------
    # Stage settings
    # -------------------------
    #st.sidebar.header("Stage Settings")
    stage_name_no_at = st.sidebar.text_input(
        "Enter stage name (e.g., MY_INT_STAGE)",
        "MY_INT_STAGE"
    )
    stage_name = f"@{stage_name_no_at}"

    # Create stage if it doesn't exist
    ensure_stage_exists(stage_name_no_at)

    # -------------------------
    # Create tabs
    # -------------------------
    tab_upload, tab_url, tab_download = st.tabs([
        "File Upload",
        "Generate Presigned URL",
        "File Download"
    ])

    # -------------------------
    # File upload tab
    # -------------------------
    with tab_upload:
        st.header("File Upload")
        st.write("Upload files to Snowflake stage.")

        uploaded_file = st.file_uploader("Choose a file")

        if uploaded_file:
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
                st.success(f"File '{uploaded_file.name}' has been uploaded successfully!")

                # Preview uploaded file
                if file_extension in PREVIEWABLE_EXTENSIONS:
                    try:
                        uploaded_file.seek(0)
                        if file_extension == '.csv':
                            try:
                                df_preview = pd.read_csv(uploaded_file)
                            except UnicodeDecodeError:
                                uploaded_file.seek(0)
                                df_preview = pd.read_csv(uploaded_file, encoding='shift-jis')
                        else:  # .txt, .tsv, etc.
                            try:
                                df_preview = pd.read_csv(uploaded_file, sep='\t')
                            except UnicodeDecodeError:
                                uploaded_file.seek(0)
                                df_preview = pd.read_csv(uploaded_file, sep='\t', encoding='shift-jis')

                        st.write("Preview of uploaded data:")
                        st.dataframe(df_preview.head())
                    except Exception as e:
                        st.warning(f"Error occurred while displaying preview: {str(e)}")
            except Exception as e:
                st.error(f"Error occurred while uploading file: {str(e)}")

    # -------------------------
    # Presigned URL generation tab
    # -------------------------
    with tab_url:
        st.header("Generate Presigned URL")
        st.write("Generate presigned URLs for files in the stage.")

        # Get list of files in stage
        stage_files = session.sql(f"LIST {stage_name}").collect()
        if stage_files:
            file_names = [
                row['name'].split('/', 1)[1] if '/' in row['name'] else row['name']
                for row in stage_files
            ]

            with st.form("url_generation_form"):
                selected_file = st.selectbox(
                    "Select a file to generate URL",
                    file_names
                )
                expiration_days = st.slider(
                    "Select expiration period (days)",
                    min_value=1,
                    max_value=7,
                    value=1,
                    help="Choose between 1 to 7 days"
                )

                submitted = st.form_submit_button("Generate URL")
                if submitted:
                    try:
                        expiration_seconds = expiration_days * 24 * 60 * 60
                        url_statement = f"""
                            SELECT GET_PRESIGNED_URL(
                                '@{stage_name_no_at}',
                                '{selected_file}',
                                {expiration_seconds}
                            )
                        """
                        result = session.sql(url_statement).collect()
                        signed_url = result[0][0]

                        st.success("URL generated successfully!")
                        st.write(f"Presigned URL (valid for {expiration_days} days):")
                        st.code(signed_url)
                    except Exception as e:
                        st.error(f"An error occurred: {str(e)}")
        else:
            st.warning("No files found in stage.")

    # -------------------------
    # File download tab
    # -------------------------
    with tab_download:
        st.header("File Download")
        st.write("Download files from stage.")

        # Get list of files in stage
        stage_files = session.sql(f"LIST {stage_name}").collect()
        if stage_files:
            file_names = [
                row['name'].split('/', 1)[1] if '/' in row['name'] else row['name']
                for row in stage_files
            ]
            selected_file = st.selectbox(
                "Select a file to download",
                file_names
            )

            if st.button("Download"):
                try:
                    with session.file.get_stream(f"{stage_name}/{selected_file}") as file_stream:
                        file_content = file_stream.read()
                        st.download_button(
                            label="Download File",
                            data=file_content,
                            file_name=selected_file,
                            mime="application/octet-stream"
                        )
                except Exception as e:
                    st.error(f"An error occurred: {str(e)}")
        else:
            st.warning("No files found in stage.")

# -------------------------------------
# Launch app
# -------------------------------------
if __name__ == "__main__":
    main() 