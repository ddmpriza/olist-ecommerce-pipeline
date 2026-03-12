import pandas as pd
import kagglehub
import sqlite3
import os


# Download latest version
path = kagglehub.dataset_download("olistbr/brazilian-ecommerce")
print("Path to dataset files:", path)


# Create a connection to a SQLite database.
# If the database file does not exist, SQLite will automatically create it.
# This database will store the raw operational tables from the Olist dataset.
conn = sqlite3.connect('olist_growth_db.db')

# Dictionary mapping logical table names to the corresponding CSV files.
# Each CSV represents a different entity of the e-commerce platform
# (customers, orders, products, payments, etc.)
files = {
        'customers': 'olist_customers_dataset.csv', 
         'orders': 'olist_orders_dataset.csv', 
         'payments': 'olist_order_payments_dataset.csv',
         'geolocation': 'olist_geolocation_dataset.csv',
         'sellers': 'olist_sellers_dataset.csv',
         'products' : 'olist_products_dataset.csv',
         'category' : 'product_category_name_translation.csv',
         'order_items': 'olist_order_items_dataset.csv',
         'reviews' : 'olist_order_reviews_dataset.csv',
         }

# Iterate over all dataset files and load them into the database
for table_name, file_name in files.items():
    # Read the CSV file into a pandas DataFrame
    # Pandas acts as the transformation layer before loading data into the database
    df = pd.read_csv(os.path.join(path, file_name))
        
    # Load the DataFrame into the SQLite database as a table.
    # if_exists='replace' ensures that if the table already exists,
    # it will be dropped and recreated with the new data.
    df.to_sql(table_name, conn, if_exists='replace', index=False)
        
    # Logging message confirming successful table creation
    print(f"Success! Table '{table_name}' is ready.")


# Close the database connection; release resources
conn.close()

