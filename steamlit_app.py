import streamlit as st
import snowflake.connector
import os
# Snowflake connection details (use Streamlit secrets in SNIS)
connections.snowflake
account = "CLQAWDG-BKB11995"
user = "AYSHAFARHANA"
password = "Amna@987654321"
role = "SYSADMIN"
warehouse = "COMPUTE_WH"
database = "SMOOTHIES"
schema = "PUBLIC"
st.title("🍓 Smoothie Orders")
name = st.text_input("Your Name:")
flavor = st.selectbox("Choose your smoothie flavor", ["Strawberry", "Banana", "Mango"])
ingredients = st.multiselect("Add extra ingredients", ["Protein Powder", "Honey", "Chia Seeds"])
if st.button("Submit Order"):
   if name:
       with conn.cursor() as cur:
           cur.execute(
               "INSERT INTO ORDERS (NAME_ON_ORDER, FLAVOR, INGREDIENTS) VALUES (%s, %s, %s)",
               (name, flavor, ", ".join(ingredients))
           )
       st.success(f"Thanks {name}! Your smoothie is on the way. 🍹")
   else:
       st.error("Please enter your name before submitting.")
