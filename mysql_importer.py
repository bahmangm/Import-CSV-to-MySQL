"""
This module provides functionality to import CSV data into a MySQL database.

The MysqlImporter class connects to a MySQL database and imports data from a specified CSV file into a specified table.
"""

import mysql.connector
import pandas as pd

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
        
        # Define the column types for the MySQL table based on the DataFrame columns
        columns = [f"{col} DATE" if col in ['date_added',] 
                   else f"{col} LONGTEXT" if col in ['cast',] 
                   else f"{col} VARCHAR(255)" 
                   for col in df.columns]

        create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(columns)});"
        self.cursor.execute(create_table_query)
        print(f"Table {table_name} created or already exists.")

        for index, row in df.iterrows():
            values = []
            for col, val in row.items():
                # Check if the column is 'date_added' and format accordingly for MySQL
                if col == 'date_added' and pd.notnull(val):
                    try:
                        # Parse the date in the format 'September 25, 2021'
                        parsed_date = pd.to_datetime(val.strip(), format='%B %d, %Y')
                        values.append(parsed_date.strftime('%Y-%m-%d'))
                    except ValueError:
                        # Set as NULL if parsing fails
                        values.append(None)
                else:
                    # For other columns, treat them as strings and escape quotes in them
                    values.append(str(val).replace('"', "'") if pd.notnull(val) else None)

            # Create parameterized insert query
            columns_str = ', '.join([f"{col}" for col in df.columns])
            placeholders = ', '.join(['%s'] * len(df.columns))
            insert_query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders});"
            
            print('='*50)
            print(insert_query)
            print('+'*50)
            # Execute the query with values as a tuple
            self.cursor.execute(insert_query, tuple(values))

        self.conn.commit()
        print("Data inserted successfully.")

    def close(self):
        """Closes the database cursor and connection."""
        self.cursor.close()
        self.conn.close()
