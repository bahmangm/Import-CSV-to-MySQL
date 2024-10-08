# CSV to MySQL Importer

This repository contains a Python utility for importing CSV data into a MySQL database. The `MysqlImporter` class facilitates the connection to a MySQL database, creating tables as needed, and inserting data from a specified CSV file.

## Features

- Connects to a MySQL database using provided credentials.
- Automatically creates a table based on the CSV file structure if it doesn't already exist.
- Infers column data types (e.g., INT, FLOAT, VARCHAR, DATE) from the first 1000 rows of the CSV file.
- Handles various data types, including dates, integers, floating-point numbers, and long text.
- Parses date-like strings and converts them into proper `DATE` fields.
- Includes error handling for CSV reading and date parsing.
- Replaces missing values (`NaN`) with `NULL` in the database.

## Usage

To use the utility, instantiate the `MysqlImporter` class with your MySQL connection details and call the `import_csv` method, specifying the target table name and CSV file path.

```python
from mysql_importer import MysqlImporter

# Example usage
importer = MysqlImporter('localhost', 'root', 'mypassword', 'portfolio')
importer.import_csv('netflix', 'netflix_titles.csv')
importer.close()
```

## Requirements

- Python 3.x
- pandas
- mysql-connector-python
- python-dateutil

## Installation

Clone the repository:

```bash
git clone https://github.com/bahmangm/Import-CSV-to-MySQL.git
```

Install the required packages:

```bash
pip install pandas mysql-connector-python python-dateutil
```
