# importers.py

import pandas as pd
import sqlite3

def import_catalog_report(df):
    catalog_columns = ["מזהה", "מקט", "מקט יצרן", "סוג פריט", "שם", "יבואן", "יצרן", "קטגוריה ראשית", "מחיר רכישה", "מחיר מכירה"]
    catalog_columns_rename = {
        "מזהה": "linet_id",
        "מקט": "sku",
        "מקט יצרן": "manufacturer_sku",
        "סוג פריט": "item_type",
        "שם": "product_name",
        "יבואן": "importer",
        "יצרן": "manufacturer",
        "קטגוריה ראשית": "main_category",
        "מחיר רכישה": "purchase_price",
        "מחיר מכירה": "consumer_price"
    }

    # Keep only the columns you need
    df = df[catalog_columns]

    # Rename the columns
    df.rename(columns=catalog_columns_rename, inplace=True)

    # Create a new SQLite connection
    conn = sqlite3.connect('sales_management.db')

    # Write to SQLite database
    df.to_sql('Products', conn, if_exists='replace', index=False)

    # Close the connection
    conn.close()

    return f"Data imported successfully to Products. The table now has {len(df)} rows."

# Other import functions can go here.
