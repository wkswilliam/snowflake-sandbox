

import pandas as pd
import streamlit as st
from snowflake.snowpark.functions import call_udf, col
from snowflake.snowpark import Session
from snowflake.snowpark import Session
from snowflake.snowpark.context import get_active_session
import os
from dotenv import load_dotenv

load_dotenv()

@st.cache_resource(show_spinner="Connecting to Snowflake...")
def getSession():
    
    try:
        return get_active_session()
    except:
        
        connection_params = {
            "account": os.getenv("account"),
            "user": os.getenv("user"),
            "password": os.getenv("password"),
            "warehouse": os.getenv("warehouse"),
            "role": os.getenv("role"),
        }
        print("\n"*5)
        print(connection_params)
        print("\n"*5)
        session = Session.builder.configs(connection_params).create()
        session.use_database("MY_APP_STREAMLIT_PYTHON_WKSWILLIAM")
        return session

def run_streamlit():
   
   st.title('Hello Snowflake!')


   st.header('UDF Example')

   st.write(
      """The sum of the two numbers is calculated by the Python add_fn() function
         which is called from core.add() UDF defined in your setup_script.sql.
      """)

   # Get the current credentials
   session = getSession()
   

   num1 = st.number_input('First number', key='numToAdd1', value=1)
   num2 = st.number_input('Second number', key='numToAdd2', value=1)

   #  Create an example data frame
   data_frame = session.create_dataframe([[num1, num2]], schema=['num1', 'num2'])
   data_frame = data_frame.select(call_udf('core.add', col('num1'), col('num2')))

   # Execute the query and convert it into a Pandas data frame
   queried_data = data_frame.to_pandas()

   # Display the Pandas data frame as a Streamlit data frame.
   st.dataframe(queried_data, use_container_width=True)


   st.header('Stored Procedure Example')

   st.write(
      """Incrementing a number by one is calculated by the Python increment_by_one_fn() function
         which implements the core.increment_by_one() Stored Procedure defined in your setup_script.sql.
      """)

   num_to_increment = st.number_input('Number to increment', key='numToIncrement', value=1)
   result = session.call('core.increment_by_one', num_to_increment)
   print("Result: "*20)
   print(result)
   st.dataframe(pd.DataFrame([[result]]), use_container_width=True)

if __name__ == '__main__':
   run_streamlit()
