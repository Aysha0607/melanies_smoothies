# ---------------------------
# streamlit_app.py  (paste all)
# ---------------------------
import streamlit as st
import pandas as pd
import requests
from urllib.parse import quote_plus
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
st.set_page_config(page_title="Custom Smoothie", layout="centered")
st.title("Customize your smoothie 🥤")
st.write("Choose the fruits you want in your custom smoothie")
# ----------- Snowflake session -----------
@st.cache_resource(show_spinner=False)
def get_session() -> Session:
   # Expects Streamlit Secrets in TOML:
   # [connections.snowflake]
   # account=""
   # user="..."
   # password="..."   (or key-based auth fields)
   # role="SYSADMIN"
   # warehouse="COMPUTE_WH"
   # database="SMOOTHIES"
   # schema="PUBLIC"
   return Session.builder.configs(st.secrets["connections"]["snowflake"]).create()
try:
   session = get_session()
except Exception as e:
   st.error("Could not connect to Snowflake. Open **Manage app → Secrets** and verify your values.")
   st.exception(e)
   st.stop()
# ----------- Helpers -----------
def oxford_join_no_oxford(items: list[str]) -> str:
   """
   'A' -> 'A'
   'A,B' -> 'A and B'
   'A,B,C' -> 'A, B and C'  (no comma before 'and' – matches DORA examples)
   """
   n = len(items)
   if n == 0:
       return ""
   if n == 1:
       return items[0]
   if n == 2:
       return f"{items[0]} and {items[1]}"
   return f"{', '.join(items[:-1])} and {items[-1]}"
def sql_escape(s: str) -> str:
   return s.replace("'", "''")
@st.cache_data(show_spinner=False)
def load_fruit_names() -> list[str]:
   df = (
       session.table("SMOOTHIES.PUBLIC.FRUIT_OPTIONS")
       .select(col("FRUIT_NAME"))
       .to_pandas()
   )
   return sorted(df["FRUIT_NAME"].tolist())
@st.cache_data(show_spinner=False)
def get_nutrition(fruit_name: str):
   # SmoothieFroot API; encode spaces etc.
   url = f"https://my.smoothiefroot.com/api/fruit/{quote_plus(fruit_name.strip().lower())}"
   r = requests.get(url, timeout=10)
   r.raise_for_status()
   data = r.json()
   return [data] if isinstance(data, dict) else data
# ----------- UI -----------
name_on_order = st.text_input("NAME ON SMOOTHIE")
st.write("NAME ON SMOOTHIE WILL BE: ", name_on_order)
try:
   fruit_choices = load_fruit_names()
except Exception as e:
   st.error("Could not load `SMOOTHIES.PUBLIC.FRUIT_OPTIONS`. Check the table/warehouse/credits.")
   st.exception(e)
   st.stop()
ingredients_list = st.multiselect("Choose up to 5 ingredients", fruit_choices, max_selections=5)
# ----------- Nutrition per fruit -----------
st.subheader("Nutrition for your chosen ingredients")
if ingredients_list:
   for fruit in ingredients_list:
       st.subheader(f"{fruit} Nutrition Information")
       try:
           rows = get_nutrition(fruit)
           if not rows:
               st.info("Sorry, that fruit is not in our database.")
               continue
           st.dataframe(pd.DataFrame(rows), use_container_width=True)
       except Exception:
           st.info("Sorry, that fruit is not in our database.")
else:
   st.caption("Pick ingredients above to see nutrition details.")
# ----------- Insert order -----------
if st.button("Submit Order"):
   if not ingredients_list:
       st.warning("Please choose at least one ingredient.")
   elif not name_on_order.strip():
       st.warning("Please enter a smoothie name.")
   else:
       # Produce the DORA-friendly text like "Apples, Lime and Ximenia"
       ing_text = oxford_join_no_oxford(ingredients_list)
       insert_sql = f"""
           INSERT INTO SMOOTHIES.PUBLIC.ORDERS (INGREDIENTS, NAME_ON_ORDER)
           VALUES ('{sql_escape(ing_text)}', '{sql_escape(name_on_order.strip())}')
       """
       try:
           session.sql(insert_sql).collect()
           st.success(f"Your Smoothie '{name_on_order}' is ordered! ✅")
       except Exception as e:
           st.error("Failed to save your order. Ensure table **SMOOTHIES.PUBLIC.ORDERS** exists "
                    "and your warehouse has credits.")
           st.exception(e)
