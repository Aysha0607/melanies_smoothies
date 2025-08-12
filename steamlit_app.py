import streamlit as st
from streamlit.connections import SnowflakeConnection
from snowflake.snowpark.functions import col
conn = st.connection("snowflake", type="snowflake")
session = conn.session()
st.set_page_config(page_title="Smoothie App", layout="centered")
st.title("Customize your smoothie 🥤")
st.caption("Choose the fruits you want in your custom smoothie")
# ---- 1) Get a Snowflake Snowpark session (inside-Snowflake & Cloud-friendly) ----
def get_session():
   try:
       # Works automatically when the app runs inside Snowflake
       from snowflake.snowpark.context import get_active_session
       s = get_active_session()
       return s
   except Exception:
       # (Only for Streamlit Cloud fallback)
       from snowflake.snowpark import Session
       s = Session.builder.configs(st.secrets).create()
       return s
session = get_session()
# ---- 2) Make sure ORDERS table exists (safe to run repeatedly) ----
try:
   session.sql("""
       CREATE TABLE IF NOT EXISTS SMOOTHIES.PUBLIC.ORDERS
       (
         INGREDIENTS    VARCHAR(200),
         NAME_ON_ORDER  VARCHAR(100)
       )
   """).collect()
except Exception as e:
   st.error("Unable to verify/create SMOOTHIES.PUBLIC.ORDERS.")
   st.exception(e)
   st.stop()
# ---- 3) Load fruit options for the multiselect ----
try:
   rows = (
       session.table("SMOOTHIES.PUBLIC.FRUIT_OPTIONS")
       .select(col("FRUIT_NAME"))
       .collect()
   )
   fruit_options = [r["FRUIT_NAME"] for r in rows]
except Exception as e:
   st.error("Could not load SMOOTHIES.PUBLIC.FRUIT_OPTIONS. Check that table exists.")
   st.exception(e)
   st.stop()
# ---- 4) Inputs ----
name_on_order = st.text_input("NAME ON SMOOTHIE")
st.write("NAME ON SMOOTHIE WILL BE:", name_on_order or "–")
ingredients_list = st.multiselect(
   "Choose up to 5 ingredients",
   fruit_options,
   max_selections=5
)
# ---- 5) Submit & insert ----
if st.button("Submit Order"):
   if not ingredients_list:
       st.warning("Please choose at least one ingredient.")
   elif not name_on_order.strip():
       st.warning("Please enter a smoothie name.")
   else:
       # Join exactly how the lab expects (comma + space)
       ingredients_string = ", ".join(ingredients_list)
       # Escape single quotes for SQL literals
       ing_sql  = ingredients_string.replace("'", "''")
       name_sql = name_on_order.strip().replace("'", "''")
       insert_sql = (
           "INSERT INTO SMOOTHIES.PUBLIC.ORDERS (INGREDIENTS, NAME_ON_ORDER) "
           f"VALUES ('{ing_sql}', '{name_sql}')"
       )
       try:
           session.sql(insert_sql).collect()
           st.success(f"Your Smoothie '{name_on_order}' is ordered! ✅")
       except Exception as e:
           st.error("Insert failed. Verify table/columns and your role/warehouse.")
           st.code(insert_sql)
           st.exception(e)
