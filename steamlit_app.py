import streamlit as st
import pandas as pd
import requests
from urllib.parse import quote_plus
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
st.set_page_config(page_title="Custom Smoothie", layout="centered")
st.title("Customize your smoothie 🥤")
st.write("Choose the fruits you want in your custom smoothie")
# ---------- Snowflake session ----------
@st.cache_resource(show_spinner=False)
def get_session() -> Session:
   return Session.builder.configs(st.secrets["connections"]["snowflake"]).create()
session = get_session()
# Connection context (writer)
with st.expander("🔎 Connection context (writer)"):
   ctx = session.sql("""
       select current_account() account,
              current_role() role,
              current_warehouse() wh,
              current_database() db,
              current_schema() schema
   """).to_pandas().iloc[0]
   st.write(dict(ctx))
   try:
       n_pending = session.sql(
           "select count(*) from SMOOTHIES.PUBLIC.ORDERS where ORDER_FILLED=false"
       ).collect()[0][0]
       st.write("Pending orders now:", int(n_pending))
   except Exception:
       st.info("ORDERS table not found yet.")
# ---------- helpers ----------
def join_for_dora(items: list[str]) -> str:
   if not items: return ""
   if len(items) == 1: return items[0]
   if len(items) == 2: return f"{items[0]} and {items[1]}"
   return f"{', '.join(items[:-1])} and {items[-1]}"
def sql_escape(s: str) -> str:
   return s.replace("'", "''")
@st.cache_data(show_spinner=False)
def load_fruits_df() -> pd.DataFrame:
   return (
       session.table("SMOOTHIES.PUBLIC.FRUIT_OPTIONS")
              .select(col("FRUIT_NAME"), col("SEARCH_ON"))
              .sort(col("FRUIT_NAME"))
              .to_pandas()
   )
@st.cache_data(show_spinner=False)
def call_smoothiefroot(search_on: str):
   url = f"https://my.smoothiefroot.com/api/fruit/{quote_plus(search_on)}"
   r = requests.get(url, timeout=10); r.raise_for_status()
   js = r.json()
   return [js] if isinstance(js, dict) else js
# ---------- UI ----------
name_on_order = st.text_input("NAME ON SMOOTHIE")
st.write("NAME ON SMOOTHIE WILL BE:", name_on_order)
fruits_df = load_fruits_df()
fruit_choices = fruits_df["FRUIT_NAME"].tolist()
ingredients_list = st.multiselect("Choose up to 5 ingredients", fruit_choices, max_selections=5)
# Nutrition panels (optional)
if ingredients_list:
   for fruit in ingredients_list:
       row = fruits_df.loc[fruits_df["FRUIT_NAME"] == fruit]
       if row.empty:
           st.info(f"No mapping found for {fruit}."); continue
       search_on = str(row["SEARCH_ON"].iloc[0]).strip()
       st.subheader(f"{fruit} • Nutrition Information")
       try:
           data = call_smoothiefroot(search_on)
           st.dataframe(pd.DataFrame(data), use_container_width=True)
       except Exception:
           st.info("Sorry, that fruit is not in our database.")
else:
   st.caption("Pick ingredients above to see nutrition details.")
# Insert order
if st.button("Submit Order"):
   if not ingredients_list:
       st.warning("Please choose at least one ingredient.")
   elif not name_on_order.strip():
       st.warning("Please enter a smoothie name.")
   else:
       ing_text = join_for_dora(ingredients_list)  # "A, B and C"
       insert_sql = f"""
           INSERT INTO SMOOTHIES.PUBLIC.ORDERS (INGREDIENTS, NAME_ON_ORDER)
           VALUES ('{sql_escape(ing_text)}', '{sql_escape(name_on_order.strip())}')
       """
       try:
           session.sql(insert_sql).collect()
           st.success(f"Your Smoothie '{name_on_order}' is ordered! ✅")
       except Exception as e:
           st.error("Failed to save your order."); st.exception(e)
