import streamlit as st
from snowflake.snowpark.functions import col
REQUIRED_KEYS = ["account","user","password","role","warehouse","database","schema"]
def get_session():
   # 1) Works when running INSIDE Snowflake
   try:
       from snowflake.snowpark.context import get_active_session
       s = get_active_session()
       if s:
           st.info("Using in-Snowflake active session.")
           return s
   except Exception:
       pass
   # 2) Streamlit Cloud: use secrets
   try:
       from snowflake.snowpark import Session
       missing = [k for k in REQUIRED_KEYS if k not in st.secrets]
       if missing:
           st.error(f"Missing Streamlit secrets: {missing}")
           st.stop()
       cfg = {k: st.secrets[k] for k in REQUIRED_KEYS}
       s = Session.builder.configs(cfg).create()
       # sanity check
       who = s.sql("select current_user(), current_account(), current_warehouse()").collect()[0]
       st.success(f"Connected as {who[0]} · acct={who[1]} · wh={who[2]}")
       return s
   except Exception as e:
       st.error("Failed to create Snowflake session. See details below.")
       st.exception(e)
       return None
session = get_session()
if session is None:
   st.stop()  # prevent 'NoneType has no attribute table' errors
st.title("Customize your smoothie 🥤")
st.write("Choose the fruits you want in your custom smoothie")
name_on_order = st.text_input("NAME ON SMOOTHIE")
st.write("NAME ON SMOOTHIE WILL BE: ", name_on_order)
# get fruit options
rows = session.table("SMOOTHIES.PUBLIC.FRUIT_OPTIONS").select(col("FRUIT_NAME")).collect()
options = [r["FRUIT_NAME"] for r in rows]
ingredients_list = st.multiselect("choose up to 5 ing", options, max_selections=5)
if st.button("Submit Order"):
   if not ingredients_list:
       st.warning("Please choose at least one ingredient.")
   elif not name_on_order.strip():
       st.warning("Please enter a smoothie name.")
   else:
       ingredients_string = ", ".join(ingredients_list)
       ing_sql  = ingredients_string.replace("'", "''")
       name_sql = name_on_order.strip().replace("'", "''")
       insert_sql = (
           "INSERT INTO SMOOTHIES.PUBLIC.ORDERS (INGREDIENTS, NAME_ON_ORDER) "
           f"VALUES ('{ing_sql}', '{name_sql}')"
       )
       session.sql(insert_sql).collect()
       st.success(f"Your Smoothie '{name_on_order}' is ordered!", icon="✅")
