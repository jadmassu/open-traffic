import os
import pandas as pd
from airflow.hooks.postgres_hook import PostgresHook
import re

def generate_create_table_query(table_name, column_names):
    try:
        column_definitions = [f'"{column}" TEXT' for column in column_names]
        create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(column_definitions)})"
        return create_table_query
    except Exception as e:
        print(f"Error generating CREATE TABLE query: {str(e)}")
        return None

def generate_insert_query(table_name, column_names):
    try:
        columns_placeholder = ', '.join(column_names)
        values_placeholder = ', '.join(['%s' for _ in column_names])
        insert_query = f"INSERT INTO {table_name} ({columns_placeholder}) VALUES ({values_placeholder})"
        return insert_query
    except Exception as e:
        print(f"Error generating INSERT query: {str(e)}")
        return None

def execute_insert_query(pg_hook, insert_query, parameters):
    try:
        pg_hook.run(insert_query, parameters)
    except Exception as e:
        print(f"Error executing INSERT query: {str(e)}")

import os
import pandas as pd
from airflow.hooks.postgres_hook import PostgresHook
import re

def generate_create_table_query(table_name, column_names):
    try:
        column_definitions = [f'"{column}" TEXT' for column in column_names]
        create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(column_definitions)})"
        return create_table_query
    except Exception as e:
        print(f"Error generating CREATE TABLE query: {str(e)}")
        return None

def generate_insert_query(table_name, column_names):
    try:
        columns_placeholder = ', '.join(column_names)
        print("columns_placeholder", columns_placeholder)
        
        values_placeholder = ', '.join(['%s' for _ in column_names])
        
        print("values_placeholder", values_placeholder)
        
        insert_query = f"INSERT INTO {table_name} ({columns_placeholder}) VALUES ({values_placeholder})"
        return insert_query
    except Exception as e:
        print(f"Error generating INSERT query: {str(e)}")
        return None
def execute_insert_query(pg_hook, insert_query, parameters):
    try:
        pg_hook.run(insert_query, parameters=parameters)
    except Exception as e:
        print(f"Error executing INSERT query: {str(e)}")
        
def execute_drop_query(pg_hook, drop_query):
    try:
        pg_hook.run(drop_query)
    except Exception as e:
        print(f"Error executing DROP TABLE query: {str(e)}")

def load_data(folder_path, table_name):
    try:
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        create_table_query = None  # Initialize the CREATE TABLE query
        # drop_table_query = f"DROP TABLE IF EXISTS {table_name}"
        # execute_drop_query(pg_hook,drop_table_query)
        for file_name in os.listdir(folder_path):
            file_path = os.path.join(folder_path, file_name)
            if file_name.endswith('.csv'):
                try:
                    data = pd.read_csv(file_path)
                    column_names = list(data.columns)
                    print()
                    
                    # Remove square brackets and their contents from column names
                    column_names = [re.sub(r'\[.*?\]', '', column).lower().replace(' ', '') for column in column_names]

                    if create_table_query is None:
                        # Generate the CREATE TABLE query only once
                        create_table_query = generate_create_table_query(table_name, column_names)
                        if create_table_query is not None:
                            pg_hook.run(create_table_query)

                    for index, row in data.iterrows():
                        parameters = list(row.values)  # Get the values from the row
                        print("parameters", parameters)
                        
                        insert_query = generate_insert_query(table_name, column_names)
                        print("insert_query", insert_query)
                        
                        if insert_query is not None:
                            execute_insert_query(pg_hook, insert_query, parameters)
                except Exception as e:
                    print(f"Error loading data from file: {file_name}")
                    print(f"Error message: {str(e)}")
    except Exception as e:
        print(f"Error connecting to the database: {str(e)}")
        
# def load_data(folder_path, table_name):
#      for file_name in os.listdir(folder_path):
#             file_path = os.path.join(folder_path, file_name)
#             if file_name.endswith('.csv'):
#                     data = pd.read_csv(file_path)
#                     column_names = list(data.columns)
#                     # print("column_names2", column_names)
                    
#                     # Remove square brackets and their contents from column names
#                     column_names = [re.sub(r'\[.*?\]', '', column) for column in column_names]
#                     # print("column_names", column_names)
                    
#                     for index, row in data.iterrows():
#                         print("rowssss", row)
                        
#                         parameters = list(row.values)  # Get the values from the row
#                         print("parameters", row)
                        
#                         insert_query = generate_insert_query(table_name, column_names)
#                         print("insert_query", insert_query)
                        
#                         if insert_query is not None:
#                             print("insert_query n------o")
#                             # execute_insert_query(pg_hook, insert_query, parameters)
                   
                    