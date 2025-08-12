import streamlit as st
from snowflake.snowpark.functions import col
import streamlit as st
try:
   import snowflake
   from snowflake.snowpark import Session
   st.write("✅ Snowflake packages imported")
except Exception as e:
   st.error(f"❌ Snowflake packages missing: {e}")
from snowflake.snowpark import Session
TEST_CONNECTION = {
   "account":   "CLQAWDG-BKB11995",  # e.g. ab12345.eu-west-1  (NO https://)
   "user":      "aysha.farhana@ecclesiastical.com",
   "password":  "Amna@1234",
   "role":      "ACCOUNTADMIN",                 # or a role that has privileges
   "warehouse": "COMPUTE_WH",                   # must exist & be started automatically
   "database":  "SMOOTHIES",
   "schema":    "PUBLIC",
}
def connect_direct(cfg):
   try:
       sess = Session.builder.configs(cfg).create()
       user, db, wh = sess.sql("select current_user(), current_database(), current_warehouse()").collect()[0]
       st.success(f"Connected as {user} · DB={db} · WH={wh}")
       return sess
   except Exception as e:
       st.exception(e)
       return None
session = connect_direct(TEST_CONNECTION)
st.title("Customize your smoothie 🥤")
st.write("Choose the fruits you want in your custom smoothie")
# Inputs
name_on_order = st.text_input("NAME ON SMOOTHIE")
st.write("NAME ON SMOOTHIE WILL BE: ", name_on_order)
fruit_df = session.table("SMOOTHIES.PUBLIC.FRUIT_OPTIONS").select(col("FRUIT_NAME"))
ingredients_list = st.multiselect("choose up to 5 ing", fruit_df, max_selections=5)
if st.button("Submit Order"):
   if not ingredients_list:
       st.warning("Please choose at least one ingredient.")
   elif not name_on_order.strip():
       st.warning("Please enter a smoothie name.")
   else:
       # Join ingredients with commas
       ingredients_string = ", ".join(ingredients_list)
       # Escape quotes for SQL safety
       ing_sql  = ingredients_string.replace("'", "''")
       name_sql = name_on_order.strip().replace("'", "''")
       # NOTE: quote "ingredients" if that column was created quoted/lowercase
       insert_sql = (
           'INSERT INTO SMOOTHIES.PUBLIC.ORDERS(INGREDIENTS, NAME_ON_ORDER) '
           f"VALUES ('{ing_sql}', '{name_sql}')"
       )
       # Uncomment for one-time debugging:
       # st.write(insert_sql); st.stop()
       session.sql(insert_sql).collect()
       st.success(f"Your Smoothie '{name_on_order}' is ordered!", icon="✅")
