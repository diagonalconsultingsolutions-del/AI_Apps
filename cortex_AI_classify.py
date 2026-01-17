import streamlit as st
from snowflake.snowpark.context import get_active_session
import pandas as pd
import json

# -------------------------------
# Setup Snowflake session
# -------------------------------
session = get_active_session()

st.title("Snowflake Classification")


def q(name):
    return f'"{name}"'

# -------------------------------
# Step 1-3: Database / Schema / Table
# -------------------------------
databases = [row["name"] for row in session.sql("SHOW DATABASES").collect()]
demo_dbs = ["JBS_DATASETS", "PROJECT_DB"]
database = st.selectbox("Select Database", demo_dbs)

schemas = [row["name"] for row in session.sql(f"SHOW SCHEMAS IN {q(database)}").collect()]
schema = st.selectbox("Select Schema", schemas)

tables = [row["name"] for row in session.sql(f"SHOW TABLES IN {q(database)}.{q(schema)}").collect()]
table = st.selectbox("Select Table", tables)

full_table = f"{q(database)}.{q(schema)}.{q(table)}"

# -------------------------------
# Step 4: Columns
# -------------------------------
column_info = session.sql(f"SHOW COLUMNS IN {full_table}").collect()
text_columns = [col["column_name"] for col in column_info]

if not text_columns:
    st.warning("No columns found in table.")
    st.stop()

selected_cols = st.multiselect("Select Text Columns", text_columns)

if selected_cols:
    df_preview = session.table(full_table).select(selected_cols).limit(5).to_pandas()
    st.markdown("### Preview of selected columns")
    st.dataframe(df_preview)

# -------------------------------
# AI_CLASSIFY Parameter Builder
# -------------------------------
def build_ai_classify_params():
    # Categories
    if "ai_categories" not in st.session_state:
        st.session_state.ai_categories = []

    st.subheader("Categories")
    with st.form("add_category", clear_on_submit=True):
        c1, c2 = st.columns([1,2])
        with c1: 
            new_label = st.text_input("Category Label")
        with c2:
            new_desc = st.text_input("Description (optional)")
        if st.form_submit_button("Add Category"):
            if new_label.strip():
                d = {"label": new_label.strip()}
                if new_desc.strip():
                    d["description"] = new_desc.strip()
                st.session_state.ai_categories.append(d)

    if st.session_state.ai_categories:
        st.json(st.session_state.ai_categories)

    # Config object
    st.subheader("Config")
    task_description = st.text_input(
        "Task description (optional)",
        placeholder="Explain what the classification should decide..."
    )
    output_mode = st.selectbox("Output mode", ["single", "multi"])

    # Examples
    if "ai_examples" not in st.session_state:
        st.session_state.ai_examples = []

    st.subheader("Examples (Optional Few-shot)")
    with st.form("add_example", clear_on_submit=True):
        ex_input = st.text_input("Input example")
        ex_labels = st.text_input("Labels (comma separated)")
        ex_explanation = st.text_input("Explanation")
        if st.form_submit_button("Add Example"):
            if ex_input.strip():
                st.session_state.ai_examples.append({
                    "input": ex_input.strip(),
                    "labels": [s.strip() for s in ex_labels.split(",")] if ex_labels else [],
                    "explanation": ex_explanation.strip()
                })
    if st.session_state.ai_examples:
        st.json(st.session_state.ai_examples)

    # Build config object
    config_object = {"output_mode": output_mode}
    if task_description:
        config_object["task_description"] = task_description
    if st.session_state.ai_examples:
        config_object["examples"] = st.session_state.ai_examples

    return st.session_state.ai_categories, config_object

categories, config = build_ai_classify_params()

# -------------------------------
# Fully dynamic Cortex run
# -------------------------------
def run_cortex(table, input_cols, categories, config_object):
    if not input_cols:
        st.warning("No input columns selected.")
        return
    if not categories:
        st.warning("No categories provided.")
        return

    # Concatenate multiple columns
    #concat_expr = " || ' ' || ".join([f'"{col}"' for col in input_cols])
    concat_expr = " || ' ' || ".join([f'COALESCE("{c}", \'\')' for c in input_cols])
    # Convert Python objects to Snowflake-compatible JSON (single quotes)
    def to_snowflake_json(obj):
        return json.dumps(obj).replace('"', "'")

    categories_json = to_snowflake_json(categories)
    config_json = to_snowflake_json(config_object)

    # Build SQL
    query = f"""
        SELECT 
            {', '.join([f'"{c}"' for c in input_cols])},
            SNOWFLAKE.CORTEX.AI_CLASSIFY(
                {concat_expr},
                {categories_json},
                {config_json}
            )
        FROM {table}
    """

    st.markdown("### Generated SQL")
    st.code(query)

    df = session.sql(query).to_pandas()
    st.markdown("### Cortex Classification Results")
    st.dataframe(df)

if st.button("Run Cortex Classification"):
    run_cortex(full_table, selected_cols, categories, config)
