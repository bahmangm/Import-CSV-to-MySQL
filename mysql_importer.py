"""
This module provides functionality to import CSV data into a MySQL database.

The MysqlImporter class connects to a MySQL database and imports data from a specified CSV file into a specified table.
"""

import mysql.connector
import pandas as pd
from dateutil.parser import parse

class MysqlImporter():
    """
    A class to facilitate the import of CSV data into a MySQL database.
    """


    def __init__(self, host, user, password, database):
        """
        Initializes the MysqlImporter with connection details.

        Args:
            host (str): Host of the MySQL server.
            user (str): User for the MySQL server.
            password (str): Password for the MySQL server.
            database (str): Database name to connect to.
        """
        # MySQL connection details
        self.host = host
        self.user = user
        self.password = password
        self.database = database

        self.conn = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        self.cursor = self.conn.cursor()
    
    
    def is_date(self,string):
        """
        Checks if the given string can be parsed as a valid date.

        Args:
            string (str): The string to check.

        Returns:
            bool: True if the string is a valid date, False otherwise.
        """
        try:
            parse(string, fuzzy=False)
            return True
        except (ValueError, TypeError):
            return False
        
    
    def to_date(self,string):
        """
        Parses the given string into a date if possible.

        Args:
            string (str): The string to parse.

        Returns:
            datetime or str: Parsed date or the original string if not a date.
        """
        try:
            return parse(string, fuzzy=False)
        except (ValueError, TypeError):
            return string
    

    def infer_column_types(self, df):
        """
        Infers MySQL-compatible column types for a DataFrame.

        Args:
            df (pandas.DataFrame): The DataFrame to infer column types from.

        Returns:
            dict: A dictionary mapping column names to inferred MySQL types.
        """
        df_sample = df.head(1000)  # Consider the first 1000 rows
        inferred_types = {}
        
        for col in df_sample.columns:
            # Use pandas inference for types
            col_type = pd.api.types.infer_dtype(df_sample[col], skipna=True)
            
            # Map inferred dtype to MySQL types
            if col_type in ['integer', 'mixed-integer']:
                inferred_types[col] = 'INT'
            elif col_type in ['floating', 'mixed-integer-float']:
                inferred_types[col] = 'FLOAT'
            else:
                # Check if the column is entirely date-like by testing each value. This check is
                # more accurate than pandas inference. For example for a string like 'September 25, 2021'.
                if df_sample[col].apply(lambda x: self.is_date(str(x)) if pd.notnull(x) else True).all():
                    inferred_types[col] = 'DATE'
                else:
                    if col_type == 'datetime64' or col_type == 'date':
                        inferred_types[col] = 'DATE'
                    elif col_type in ['string', 'mixed', 'unicode']:
                        inferred_types[col] = 'VARCHAR(255)' if df_sample[col].str.len().max() <= 255 else 'LONGTEXT'
                    else:
                        inferred_types[col] = 'VARCHAR(255)'  # Fallback

        return inferred_types

    
    def import_csv(self, table_name, csv_file_path):
        """
        Imports data from a CSV file into a specified MySQL table.

        Args:
            table_name (str): The name of the table where data will be imported.
            csv_file_path (str): The file path of the CSV file to be imported.
        """

        try:
            df = pd.read_csv(csv_file_path)
        except:
            df = pd.read_csv(csv_file_path, encoding='ISO-8859-1')
        
        # Replace NaN values with None (equivalent to NULL in MySQL)
        df = df.where(pd.notnull(df), None)

        # Define the column types for the MySQL table based on the DataFrame columns
        columns_types = self.infer_column_types(df)

        print('The type of each column:')
        print('Column', ' : --> ', 'Type')
        for column, col_type in columns_types.items():
            print(column, ' : --> ', col_type)
        print('-'*40)    
        
        columns = [f"{k} {v}" for k,v in columns_types.items()]
        create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(columns)});"
        self.cursor.execute(create_table_query)
        print(f"Table {table_name} created or already exists.")

        for index, row in df.iterrows():
            values = []
            for col, val in row.items():
                if columns_types[col] == 'DATE':
                    values.append(self.to_date(val) if (val is not None and pd.notnull(val)) else None)

                elif columns_types[col] == 'INT':
                    values.append(int(val) if (val is not None and pd.notnull(val)) else None)
                elif columns_types[col] == 'FLOAT':
                    values.append(float(val) if (val is not None and pd.notnull(val)) else None)
                else:
                    # For other types, treat them as strings and escape quotes in them
                    values.append(str(val).replace('"', "'") if pd.notnull(val) else None)

            # Create parameterized insert query
            columns_str = ', '.join([f"{col}" for col in df.columns])
            placeholders = ', '.join(['%s'] * len(df.columns))
            insert_query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders});"
            
            # Execute the query with values as a tuple
            self.cursor.execute(insert_query, tuple(values))

        self.conn.commit()
        print("Data inserted successfully.")

    def close(self):
        """Closes the database cursor and connection."""
        self.cursor.close()
        self.conn.close()
