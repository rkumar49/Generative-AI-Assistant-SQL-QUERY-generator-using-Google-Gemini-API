import pandas as pd
import google.generativeai as genai
import streamlit as st

# Configure the Google API with the API key directly
genai.configure(api_key="your api key here")

# Function to generate the prompt for the Gemini model
def generate_prompt(tables_info):
    prompt = """
    You are an expert SQL Specialist. Your job is to convert English or natural language to SQL Query.
    You can perform necessary joins, unions, and other SQL operations where required.
    Also, the SQL code should not have ``` in the beginning or end and SQL word on the output.
    """

    for table_info in tables_info:
        table_name = table_info['table_name']
        columns = ', '.join(table_info['columns'])
        prompt += f"""
        I have a table called {table_name} which has columns like {columns}.
        When preparing the SQL query, consider the column names mentioned above.
        Do not add extra information except what is asked!
        When giving output, make sure to use user-friendly aliases.
        Table Name: {table_name}.

        Example 1: How many records are active in {table_name}?
        Answer: SELECT COUNT(*) FROM {table_name} WHERE is_active = 'Y';
        Example 2: Retrieve the descriptions of all records in {table_name}.
        Answer: SELECT description FROM {table_name};
        Example 3: Retrieve the records created by a specific user in {table_name}.
        Answer: SELECT * FROM {table_name} WHERE created_by = 'specific_user';
        Example 4: Retrieve all information for records updated on a specific date in {table_name}.
        Answer: SELECT * FROM {table_name} WHERE updated = 'specific_date';
        Example 5: Retrieve the records where 'property_value' > 100.
        Answer: SELECT * FROM {table_name} WHERE property_value > 100;
        Example 6: List the names of all categories that are active.
        Answer: SELECT category FROM EBA_CUST_CATEGORIES WHERE is_active = 'Y';
        Example 7: Show all records from EBA_CUST_ACTIVITIES and EBA_CUST_CATEGORIES joined on 'type_id'.
        Answer: SELECT a.*, c.category FROM EBA_CUST_ACTIVITIES a INNER JOIN EBA_CUST_CATEGORIES c ON a.type_id = c.id;
        Example 8: Get all the descriptions from EBA_CUST_ACTIVITIES and EBA_CUST_CATEGORIES for type_id = 2.
        Answer: SELECT a.description, c.description FROM EBA_CUST_ACTIVITIES a INNER JOIN EBA_CUST_CATEGORIES c ON a.type_id = c.id WHERE a.type_id = 2;
        Example 9: Retrieve earthquake records from USGS_LOCAL with magnitude greater than 2.0.
        Answer: SELECT * FROM USGS_LOCAL WHERE properties_mag > 2.0;
        Example 10: Get the place and title of earthquakes from USGS_LOCAL with a magnitude greater than 1.5.
        Answer: SELECT properties_place, properties_title FROM USGS_LOCAL WHERE properties_mag > 1.5;
        Example 11: Retrieve records where 'properties_sig' is greater than 19.
        Answer: SELECT * FROM USGS_LOCAL WHERE properties_sig > 19;
        Example 12: Get the list of all unique categories.
        Answer: SELECT DISTINCT category FROM EBA_CUST_CATEGORIES;
        Example 13: Find all activities with 'type_id' greater than 1.
        Answer: SELECT * FROM EBA_CUST_ACTIVITIES WHERE type_id > 1;
        Example 14: Retrieve the average magnitude of earthquakes.
        Answer: SELECT AVG(properties_mag) FROM USGS_LOCAL;
        """
    
    return [prompt]

# Function to generate SQL query using Google Gemini API
def get_response(question, prompt):
    model = genai.GenerativeModel('gemini-pro')
    response = model.generate_content([prompt[0], question])
    sql_query = response.text.strip()  # Ensure to strip any leading/trailing whitespace
    # Remove unwanted backticks and ensure SQL query format
    sql_query = sql_query.replace('```', '')
    return sql_query

# Function to execute SQL-like query on pandas DataFrame
def execute_sql_query_on_df(query, dfs):
    try:
        # Extract the table name from the query
        table_name = query.split("FROM")[1].split()[0].strip().upper()
        if table_name not in dfs:
            st.error(f"Table '{table_name}' not found.")
            return pd.DataFrame()
        
        # Get the DataFrame for the table
        df = dfs[table_name]

        # Adjust query string for pandas execution
        sql_query = query.strip()
        # Check if the query starts with SELECT
        if sql_query.startswith("SELECT"):
            # Handle potential multi-line expressions
            if '\n' in sql_query:
                sql_query = sql_query.replace('\n', ' ')
                
            # Convert SQL WHERE conditions for pandas
            if 'WHERE' in sql_query:
                select_part = sql_query.split('SELECT')[1].split('FROM')[0].strip()
                from_part = sql_query.split('FROM')[1].split('WHERE')[0].strip()
                where_part = sql_query.split('WHERE')[1].strip()
                
                # If select_part is empty, assume '*'
                if not select_part:
                    select_part = '*'
                
                # Handle joins
                if 'JOIN' in from_part:
                    tables = from_part.split('JOIN')
                    main_table = tables[0].strip()
                    join_table = tables[1].split('ON')[0].strip()
                    join_condition = tables[1].split('ON')[1].strip()

                    # Perform the join
                    df_main = dfs[main_table]
                    df_join = dfs[join_table]
                    joined_df = pd.merge(df_main, df_join, left_on=join_condition.split('=')[0].strip(), 
                                         right_on=join_condition.split('=')[1].strip(), 
                                         how='inner')
                    # Execute the query with where conditions
                    result = joined_df.query(where_part)
                else:
                    # Adjust WHERE condition syntax for pandas
                    where_part = where_part.replace('=', '==').replace('\'', '"')
                    result = df.query(where_part)
                
                if select_part != '*':
                    columns = [col.strip() for col in select_part.split(',')]
                    result = result[columns]

            else:
                result = df
        else:
            st.error(f"Invalid SQL query format: {sql_query}")
            return pd.DataFrame()
        
        return result
    except Exception as e:
        st.error(f"Error executing the query: {str(e)}")
        return pd.DataFrame()

# Initialize the app
st.image("logo.png", width=100)
st.header("Data Analysis Bot: App to Retrieve SQL/Tabular Data!")

# Upload multiple CSV files
uploaded_files = st.file_uploader("Choose CSV files", accept_multiple_files=True, type="csv")

# Prepare table information from uploaded CSV files
tables_info = []
dfs = {}
if uploaded_files:
    for file in uploaded_files:
        df = pd.read_csv(file)
        table_name = file.name.split('.')[0].upper()  # Use the filename (without extension) as the table name
        df.name = table_name  # Assign the table name to the DataFrame
        tables_info.append({
            "table_name": table_name,
            "columns": list(df.columns)
        })
        dfs[table_name] = df  # Store DataFrame in a dictionary

# Text area & Button
question = st.text_input("Ask your question:")

# If the button is clicked
if question:
    if tables_info:
        # Generate the prompt based on CSV files
        prompt = generate_prompt(tables_info)
        
        # Generate the SQL query
        sql_query = get_response(question, prompt)
        st.write(f"The Generated SQL query is:\n{sql_query}")

        # Execute the SQL Query on the appropriate DataFrame
        if sql_query:
            result = execute_sql_query_on_df(sql_query, dfs)
            if not result.empty:
                st.subheader(f"Results:")
                st.write(result)
            else:
                st.write("No results found for the query or there was an error executing the query.")
    else:
        st.error("Please upload at least one CSV file.")
