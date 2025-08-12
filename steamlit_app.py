import streamlit as st
from snowflake.snowpark.functions import col
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
