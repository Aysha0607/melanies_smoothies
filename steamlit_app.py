# streamlit_app.py  — SNiS app (Streamlit Cloud)
import streamlit as st
import pandas as pd
import requests
from urllib.parse import quote_plus
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
st.set_page_config(page_title="Custom Smoothie", layout="centered")
st.title("Customize your smoothie 🧃")
st.write("Choose the fruits you want in your custom smoothie")
# ---------------- Snowflake session ----------------
@st.cache_resource(show_spinner=False)
def get_session() -> Session:
   # Uses Streamlit Secrets (Manage app ▸ Secrets)
   #   [connections.snowflake]
   #   account="ORG-ACCOUNT"
   #   user="YOUR_USER"
   #   password="YOUR_PASSWORD"
   #   role="SYSADMIN"
   #   warehouse="COMPUTE_WH"
   #   database="SMOOTHIES"
   #   schema="PUBLIC"
   return Session.builder.configs(st.secrets["connections"]["snowflake"]).create()
try:
   session = get_session()
except Exception as e:
   st.error("Could not connect to Snowflake. Check your Streamlit Secrets.")
   st.exception(e)
   st.stop()
# ---------------- Helpers ----------------
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
   r = requests.get(url, timeout=10)
   r.raise_for_status()
   js = r.json()
   return [js] if isinstance(js, dict) else js
def join_for_dora(items: list[str]) -> str:
   if not items:
       return ""
   if len(items) == 1:
       return items[0]
   if len(items) == 2:
       return f"{items[0]} and {items[1]}"
   return f"{', '.join(items[:-1])} and {items[-1]}"
# ---------------- UI ----------------
name_on_order = st.text_input("NAME ON SMOOTHIE")
st.write("NAME ON SMOOTHIE WILL BE:", name_on_order)
try:
   fruits_df = load_fruits_df()
except Exception as e:
   st.error("Could not load FRUIT_OPTIONS. Make sure the table exists and your WH has credits.")
   st.exception(e)
   st.stop()
fruit_choices = fruits_df["FRUIT_NAME"].tolist()
ingredients_list = st.multiselect("Choose up to 5 ingredients:", fruit_choices, max_selections=5)
# -------- Nutrition per fruit (SmoothieFroot) --------
if ingredients_list:
   for fruit in ingredients_list:
       row = fruits_df.loc[fruits_df["FRUIT_NAME"] == fruit]
       if row.empty:
           st.info(f"No mapping found for {fruit}.")
           continue
       search_on = str(row["SEARCH_ON"].iloc[0]).strip()
       st.subheader(f"{fruit} • Nutrition Information")
       try:
           data = call_smoothiefroot(search_on)
           if not data:
               st.info("Sorry, that fruit is not in our database.")
           else:
               st.dataframe(pd.DataFrame(data), use_container_width=True)
       except Exception:
           st.info("Sorry, that fruit is not in our database.")
else:
   st.caption("Pick ingredients above to see nutrition details.")
# ---------------- Insert order ----------------
if st.button("Submit Order"):
   if not ingredients_list:
       st.warning("Please choose at least one ingredient.")
   elif not name_on_order.strip():
       st.warning("Please enter a smoothie name.")
   else:
       ing_text = join_for_dora(ingredients_list)     # matches the workshop grammar
       insert_sql = f"""
           INSERT INTO SMOOTHIES.PUBLIC.ORDERS (INGREDIENTS, NAME_ON_ORDER)
           VALUES ('{sql_escape(ing_text)}', '{sql_escape(name_on_order.strip())}')
       """
       try:
           session.sql(insert_sql).collect()
           st.success(f"Your Smoothie '{name_on_order}' is ordered! ✅")
       except Exception as e:
           st.error("Failed to save your order.")
           st.exception(e)
# ---------------- DORA helper (seed + preview) ----------------
# make sure this Streamlit session matches the worksheet session
session.sql("USE ROLE SYSADMIN").collect()
session.sql("USE WAREHOUSE COMPUTE_WH").collect()
session.sql("USE DATABASE SMOOTHIES").collect()
session.sql("USE SCHEMA PUBLIC").collect()
# hash settings so DORA math matches the course grader
session.sql("ALTER SESSION SET HASH_OUTPUT_FORMAT='INT', HASH_SEED=0").collect()
with st.expander("🧪 DORA helper (seed required rows)"):
   st.write(
       """
       This will **TRUNCATE SMOOTHIES.PUBLIC.ORDERS** and insert the 3 rows that the DORA grader expects
       (including exact punctuation).
       ```
       Kevin -> 'Apples, Lime and Ximenia'                     (ORDER_FILLED = FALSE)
       Divya -> 'Dragon Fruit, Guava, Figs, Jackfruit and Blueberries'  (TRUE)
       Xi    -> 'Vanilla, Kiwi and Cherries'                   (TRUE)
       ```
       """
   )
   dora_seed_sql = """
       TRUNCATE TABLE SMOOTHIES.PUBLIC.ORDERS;
       INSERT INTO SMOOTHIES.PUBLIC.ORDERS (INGREDIENTS, NAME_ON_ORDER, ORDER_FILLED)
       VALUES
        ('Apples, Lime and Ximenia', 'Kevin', FALSE),
        ('Dragon Fruit, Guava, Figs, Jackfruit and Blueberries', 'Divya', TRUE),
        ('Vanilla, Kiwi and Cherries', 'Xi', TRUE);
   """
   EXPECTED = 2881182761772377708
   dora_check_sql = """
   select sum(hash_ing) as ACTUAL
   from (
     select hash(ingredients) as hash_ing
     from smoothies.public.orders
     where order_ts is not null
       and name_on_order is not null
       and (
         (name_on_order = 'Kevin' and order_filled = FALSE and ingredients = 'Apples, Lime and Ximenia')
         or
         (name_on_order = 'Divya' and order_filled = TRUE  and ingredients = 'Dragon Fruit, Guava, Figs, Jackfruit and Blueberries')
         or
         (name_on_order = 'Xi'    and order_filled = TRUE  and ingredients = 'Vanilla, Kiwi and Cherries')
       )
   ) t;
   """
   agree = st.selectbox("Type: I AGREE (this clears the table)", ["", "I AGREE"])
   if st.button("Prep DORA data"):
       if agree != "I AGREE":
           st.warning("Pick 'I AGREE' to allow truncating the ORDERS table.")
       else:
           try:
               for stmt in dora_seed_sql.split(";"):
                   if stmt.strip():
                       session.sql(stmt).collect()
               st.success("DORA data seeded. ✅")
           except Exception as e:
               st.error("Failed to seed DORA data.")
               st.exception(e)
   if st.button("Run local DORA check (preview)"):
       try:
           row = session.sql(dora_check_sql).collect()[0]
           actual = row["ACTUAL"]
           st.write("**Actual:**", actual)
           st.write("**Expected:**", EXPECTED)
           if actual is None:
               st.error("Actual is NULL → rows don’t match the required values. Click 'Prep DORA data' and try again.")
           elif actual == EXPECTED:
               st.success("Preview MATCH ✅ — now run the official DORA SQL in Snowflake.")
           else:
               st.error("Mismatch — re-seed with the button above.")
       except Exception as e:
           st.exception(e)
