import streamlit as st
from snowflake.snowpark import Session
import pandas as pd
# Function to create a Snowflake session
def get_session():
   return Session.builder.configs(st.secrets["connections"]["snowflake"]).create()
# Create a session
session = get_session()
st.title("🥤 Customize your smoothie")
st.write("Choose the fruits you want in your custom smoothie")
# Load fruit options from Snowflake
try:
   fruit_df = session.table("SMOOTHIES.PUBLIC.FRUIT_OPTIONS").to_pandas()
except Exception as e:
   st.error(f"Could not load SMOOTHIES.PUBLIC.FRUIT_OPTIONS: {e}")
   st.stop()
# Multiselect for fruits
ingredients_list = st.multiselect(
   "Choose ingredients:",
   fruit_df["FRUIT_NAME"].tolist()
)
# Name on order
name_on_order = st.text_input("Name on smoothie:")
# Submit button
if st.button("Submit Order"):
   if not ingredients_list:
       st.warning("Please select at least one fruit.")
   elif not name_on_order:
       st.warning("Please enter a name for the order.")
   else:
       st.success(f"Order submitted for {name_on_order} with {', '.join(ingredients_list)}!")
