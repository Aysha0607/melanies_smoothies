import streamlit as st
import pandas as pd
import requests
from snowflake.snowpark import Session
import json
# ---------------- Snowflake Connection ----------------
def create_session():
   connection_parameters = {
       "account": st.secrets["snowflake"]["account"],
       "user": st.secrets["snowflake"]["user"],
       "password": st.secrets["snowflake"]["password"],
       "role": st.secrets["snowflake"]["role"],
       "warehouse": st.secrets["snowflake"]["warehouse"],
       "database": st.secrets["snowflake"]["database"],
       "schema": st.secrets["snowflake"]["schema"]
   }
   return Session.builder.configs(connection_parameters).create()
# ---------------- Page Title ----------------
st.title("🍓 Customize Your Smoothie! 🍍")
# ---------------- Load Fruit Options from Snowflake ----------------
try:
   session = create_session()
   fruit_df = session.table("SMOOTHIES.PUBLIC.FRUIT_OPTIONS").to_pandas()
   fruit_list = fruit_df['FRUIT_NAME'].tolist()
except Exception as e:
   st.error(f"Could not load fruit list from Snowflake: {e}")
   fruit_list = []
# ---------------- Form Inputs ----------------
name_on_smoothie = st.text_input("Name on Smoothie:")
ingredients_list = st.multiselect(
   "Choose up to 5 ingredients:",
   options=fruit_list,
   max_selections=5
)
# ---------------- Demo: Watermelon API Call ----------------
try:
   demo_resp = requests.get("https://my.smoothiefroot.com/api/fruit/watermelon", timeout=10)
   st.write("Demo API call:", demo_resp)
   st.write("Raw JSON:", demo_resp.json())
   st.dataframe(pd.DataFrame(demo_resp.json()), use_container_width=True)
except Exception as e:
   st.warning(f"Demo API call failed: {e}")
# ---------------- API Nutrition for Selected Ingredients ----------------
st.subheader("Nutrition for your chosen ingredients")
@st.cache_data(show_spinner=False)
def fetch_fruit_data(fruit_name: str):
   url = f"https://my.smoothiefroot.com/api/fruit/{fruit_name.strip().lower()}"
   r = requests.get(url, timeout=10)
   r.raise_for_status()
   return r.json()
if ingredients_list:
   all_data = []
   for fruit in ingredients_list:
       try:
           fruit_data = fetch_fruit_data(fruit)
           for row in fruit_data:
               row["chosen"] = fruit
               all_data.append(row)
       except Exception as e:
           st.error(f"Error fetching {fruit}: {e}")
   if all_data:
       df = pd.DataFrame(all_data)
       st.dataframe(df, use_container_width=True)
else:
   st.caption("Pick ingredients above to see nutrition details.")
