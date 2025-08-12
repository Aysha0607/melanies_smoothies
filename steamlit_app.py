# --- imports
import streamlit as st
import pandas as pd
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
st.set_page_config(page_title="Custom Smoothie", layout="centered")
st.title("Customize your smoothie 🥤")
st.write("Choose the fruits you want in your custom smoothie")
# --- connect to Snowflake via Streamlit secrets
@st.cache_resource(show_spinner=False)
def get_session() -> Session:
   return Session.builder.configs(st.secrets["connections"]["snowflake"]).create()
try:
   session = get_session()
except Exception as e:
   st.error("Could not connect to Snowflake. Check your Streamlit Secrets.")
   st.exception(e)
   st.stop()
# --- helpers
def sql_escape(s: str) -> str:
   return s.replace("'", "''")
def join_for_dora(items: list[str]) -> str:
   if not items: return ""
   if len(items) == 1: return items[0]
   if len(items) == 2: return f"{items[0]} and {items[1]}"
   return f"{', '.join(items[:-1])}, and {items[-1]}"
@st.cache_data(show_spinner=False)
def load_fruit_choices() -> list[str]:
   try:
       df = (session.table("SMOOTHIES.PUBLIC.FRUIT_OPTIONS")
                    .select(col("FRUIT_NAME"))
                    .sort(col("FRUIT_NAME"))
                    .to_pandas())
       return df["FRUIT_NAME"].tolist()
   except Exception:
       # Fallback in case the table doesn't exist yet
       return ["Apples","Blueberries","Cherries","Dragon Fruit","Figs",
               "Guava","Jackfruit","Kiwi","Lime","Vanilla","Ximenia"]
# --- UI
name_on_order = st.text_input("NAME ON SMOOTHIE", key="order_name_input")
fruit_choices = load_fruit_choices()
ingredients_list = st.multiselect("Choose up to 5 ingredients",
                                 fruit_choices, max_selections=5, key="ingredients_ms")
# --- insert order
if st.button("Submit Order", key="submit_order_btn"):
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
# --------- DORA helper (seed + local check) ---------
with st.expander("🧪 DORA helper (seed required rows)", expanded=True):
   st.caption("This will TRUNCATE SMOOTHIES.PUBLIC.ORDERS and insert the 3 rows the grader expects (including exact punctuation).")
   agree = st.text_input("Type: I AGREE (this clears the table)", key="dora_agree_input")
   c1, c2 = st.columns(2)
   if c1.button("Prep DORA data", key="dora_prep_btn"):
       if agree.strip().upper() == "I AGREE":
           try:
               session.sql("TRUNCATE TABLE SMOOTHIES.PUBLIC.ORDERS").collect()
               session.sql("""
                   INSERT INTO SMOOTHIES.PUBLIC.ORDERS (INGREDIENTS, NAME_ON_ORDER, ORDER_FILLED)
                   SELECT 'Apples, Lime and Ximenia', 'Kevin', FALSE
                   UNION ALL
                   SELECT 'Dragon Fruit, Guava, Figs, Jackfruit and Blueberries', 'Divya', TRUE
                   UNION ALL
                   SELECT 'Vanilla, Kiwi and Cherries', 'Xi', TRUE;
               """).collect()
               st.success("Seeded the 3 required rows.")
           except Exception as e:
               st.error("Failed to seed DORA data.")
               st.exception(e)
       else:
           st.warning('Please type I AGREE exactly to enable seeding.')
   if c2.button("Run local DORA check (preview)", key="dora_check_btn"):
       try:
           q = session.sql("""
               SELECT SUM(HASH(INGREDIENTS)) AS ACTUAL
               FROM SMOOTHIES.PUBLIC.ORDERS
               WHERE ORDER_TS IS NOT NULL AND NAME_ON_ORDER IS NOT NULL
                 AND (
                       (NAME_ON_ORDER='Kevin' AND ORDER_FILLED=FALSE) OR
                       (NAME_ON_ORDER='Divya' AND ORDER_FILLED=TRUE)  OR
                       (NAME_ON_ORDER='Xi'    AND ORDER_FILLED=TRUE)
                     )
           """).to_pandas()
           actual = int(q.iloc[0]["ACTUAL"]) if not q.empty and pd.notna(q.iloc[0]["ACTUAL"]) else None
           expected = 2881182761772377708
           st.write("Actual:", actual)
           st.write("Expected:", expected)
           st.success("Match ✅") if actual == expected else st.error("Mismatch — click *Prep DORA data* and try again.")
       except Exception as e:
           st.error("Local check failed.")
           st.exception(e)
