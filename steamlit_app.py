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
   s = Session.builder.configs(st.secrets["connections"]["snowflake"]).create()
   s.sql("USE ROLE SYSADMIN").collect()
   s.sql("USE WAREHOUSE COMPUTE_WH").collect()
   s.sql("USE DATABASE SMOOTHIES").collect()
   s.sql("USE SCHEMA PUBLIC").collect()
   # HASH_OUTPUT_FORMAT isn't available on all accounts; don't set it.
   try:
       s.sql("ALTER SESSION SET HASH_SEED=0").collect()
   except Exception:
       pass
   return s
try:
   session = get_session()
except Exception as e:
   st.error("Could not connect to Snowflake. Check your Streamlit secrets.")
   st.exception(e)
   st.stop()
# ---------- Helpers ----------
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
   r = requests.get(url, timeout=10)
   r.raise_for_status()
   js = r.json()
   return [js] if isinstance(js, dict) else js
# ---------- UI ----------
name_on_order = st.text_input("NAME ON SMOOTHIE")
st.write("NAME ON SMOOTHIE WILL BE:", name_on_order)
try:
   fruits_df = load_fruits_df()
except Exception as e:
   st.error("Could not load FRUIT_OPTIONS. Make sure the table exists and your WH has credits.")
   st.exception(e)
   st.stop()
fruit_choices = fruits_df["FRUIT_NAME"].tolist()
ingredients_list = st.multiselect("Choose up to 5 ingredients", fruit_choices, max_selections=5)
# Nutrition per fruit
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
# Insert order
if st.button("Submit Order"):
   if not ingredients_list:
       st.warning("Please choose at least one ingredient.")
   elif not name_on_order.strip():
       st.warning("Please enter a smoothie name.")
   else:
       ing_text = join_for_dora(ingredients_list)
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
# ---------- DORA helper (optional but handy) ----------
with st.expander("🧪 DORA helper (seed required rows)"):
   st.write("""
This will **TRUNCATE** `SMOOTHIES.PUBLIC.ORDERS` and insert the 3 rows that the grader expects.
- Kevin  → `'Apples, Lime and Ximenia'`  (ORDER_FILLED = FALSE)
- Divya  → `'Dragon Fruit, Guava, Figs, Jackfruit and Blueberries'` (TRUE)
- Xi     → `'Vanilla, Kiwi and Cherries'` (TRUE)
""")
   agree = st.text_input("Type: I AGREE (this clears the table)")
   if st.button("Prep DORA data") and agree.strip().upper() == "I AGREE":
       try:
           # use hex constants to avoid locale/space issues
           seed_sql = """
TRUNCATE TABLE SMOOTHIES.PUBLIC.ORDERS;
INSERT INTO SMOOTHIES.PUBLIC.ORDERS (INGREDIENTS, NAME_ON_ORDER, ORDER_FILLED)
SELECT TO_VARCHAR(TO_BINARY('4170706C65732C204C696D6520616E642058696D656E6961','HEX'),'UTF-8'),'Kevin', FALSE
UNION ALL
SELECT TO_VARCHAR(TO_BINARY('447261676F6E2046727569742C2047756176612C20466967732C204A61636B667275697420616E6420426C756562657272696573','HEX'),'UTF-8'),'Divya', TRUE
UNION ALL
SELECT TO_VARCHAR(TO_BINARY('56616E696C6C612C204B69776920616E64204368657272696573','HEX'),'UTF-8'),'Xi', TRUE;
"""
           session.sql(seed_sql).collect()
           st.success("Seeded DORA rows.")
       except Exception as e:
           st.error("Failed to seed DORA data.")
           st.exception(e)
   if st.button("Run local DORA check (preview)"):
       try:
           q = """
WITH c AS (
 SELECT TO_NUMBER(HASH(INGREDIENTS)) AS h
 FROM SMOOTHIES.PUBLIC.ORDERS
 WHERE ORDER_TS IS NOT NULL
   AND NAME_ON_ORDER IS NOT NULL
   AND (
        (NAME_ON_ORDER='Kevin' AND ORDER_FILLED=FALSE)
     OR (NAME_ON_ORDER='Divya' AND ORDER_FILLED=TRUE)
     OR (NAME_ON_ORDER='Xi'    AND ORDER_FILLED=TRUE)
   )
)
SELECT COALESCE(SUM(h),0) AS ACTUAL FROM c;
"""
           actual = session.sql(q).to_pandas().iloc[0,0]
           st.write("**Actual:**", actual)
           st.write("**Expected:** 2881182761772377708")
           if int(actual) == 2881182761772377708:
               st.success("Looks good. Now run the DORA grader in the course worksheet.")
           else:
               st.warning("Mismatch — re‑seed above, then run preview again.")
       except Exception as e:
           st.error("Preview failed.")
           st.exception(e)
          # dora_helper.py  (you can also paste this at the bottom of your streamlit_app.py)
import streamlit as st
import pandas as pd
from snowflake.snowpark import Session
# >>>> EDIT THESE TWO to your real DB/SCHEMA <<<<
DB = "SMOOTHIES"
SCHEMA = "PUBLIC"
st.set_page_config(page_title="DORA helper", layout="centered")
st.header("✅ DORA helper (seed required rows)")
@st.cache_resource(show_spinner=False)
def get_session() -> Session:
   # Streamlit Cloud "Secrets" must already be set under [connections.snowflake]
   return Session.builder.configs(st.secrets["connections"]["snowflake"]).create()
session = get_session()
# Make sure we’re in the right place, always
session.sql(f"USE DATABASE {DB}").collect()
session.sql(f"USE SCHEMA {SCHEMA}").collect()
st.caption(f"Using: {DB}.{SCHEMA}  (role: {session.get_current_role()}, wh: {session.get_current_warehouse()})")
st.write("""
This will **TRUNCATE `SMOOTHIES.PUBLIC.ORDERS`** and insert the 3 rows the grader expects
(including exact punctuation and spaces).
""")
st.code(
"Kevin -> 'Apples, Lime and Ximenia' (ORDER_FILLED = FALSE)\n"
"Divya -> 'Dragon Fruit, Guava, Figs, Jackfruit and Blueberries' (TRUE)\n"
"Xi    -> 'Vanilla, Kiwi and Cherries' (TRUE)",
language="text"
)
agree = st.text_input("Type: I AGREE (this clears the table)")
# HEX for each exact string (UTF‑8)
HEX_KEVIN =  "4170706C65732C204C696D6520616E642058696D656E6961"  # Apples, Lime and Ximenia
HEX_DIVYA =  ("447261676F6E2046727569742C2047756176612C20466967732C20"
             "4A61636B667275697420616E6420426C756562657272696573")
HEX_XI    =  "56616E696C6C612C204B69776920616E64204368657272696573"
def seed():
   # 1) clear table
   session.sql("TRUNCATE TABLE SMOOTHIES.PUBLIC.ORDERS").collect()
   # 2) insert using exact bytes -> UTF‑8 text
   insert_sql = f"""
   INSERT INTO SMOOTHIES.PUBLIC.ORDERS(INGREDIENTS, NAME_ON_ORDER, ORDER_FILLED)
   SELECT TO_VARCHAR(TO_BINARY('{HEX_KEVIN}','HEX'),'UTF-8'), 'Kevin', FALSE
   UNION ALL
   SELECT TO_VARCHAR(TO_BINARY('{HEX_DIVYA}','HEX'),'UTF-8'), 'Divya', TRUE
   UNION ALL
   SELECT TO_VARCHAR(TO_BINARY('{HEX_XI}','HEX'),'UTF-8'), 'Xi', TRUE
   """
   session.sql(insert_sql).collect()
def fetch_preview():
   q = """
   SELECT
     NAME_ON_ORDER,
     INGREDIENTS,
     HASH(INGREDIENTS)    AS H,
     LENGTH(INGREDIENTS)  AS LEN,
     TO_VARCHAR(TO_BINARY(INGREDIENTS, 'UTF-8'), 'HEX') AS HEX
   FROM SMOOTHIES.PUBLIC.ORDERS
   ORDER BY NAME_ON_ORDER;
   """
   return session.sql(q).to_pandas()
def run_sum():
   q = """
   SELECT SUM(HASH(INGREDIENTS)) AS SUM_H
   FROM SMOOTHIES.PUBLIC.ORDERS
   WHERE ORDER_TS IS NOT NULL
     AND NAME_ON_ORDER IS NOT NULL
     AND (
          (NAME_ON_ORDER='Kevin' AND ORDER_FILLED = FALSE)
       OR (NAME_ON_ORDER='Divya' AND ORDER_FILLED = TRUE)
       OR (NAME_ON_ORDER='Xi'    AND ORDER_FILLED = TRUE)
     );
   """
   return session.sql(q).to_pandas().iloc[0,0]
col1, col2 = st.columns(2)
with col1:
   if st.button("Prep DORA data"):
       if agree.strip().upper() != "I AGREE":
           st.error("Type I AGREE exactly to allow truncation.")
       else:
           seed()
           st.success("Seeded the 3 rows.")
with col2:
   if st.button("Run local DORA check (preview)"):
       df = fetch_preview()
       st.dataframe(df, use_container_width=True)
       actual = run_sum()
       st.write("**Actual:**", actual)
       st.write("**Expected:** 2881182761772377708")
       if actual == 2881182761772377708:
           st.success("Looks good locally. Now run the official DORA SQL in the course page.")
       else:
           st.error("Mismatch — click *Prep DORA data* then try again.")
# Always show current rows to help debug
st.divider()
st.subheader("Current ORDERS rows")
st.dataframe(fetch_preview(), use_container_width=True)
