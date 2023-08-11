import pandas as pd
import numpy as np
import sqlite3
from flask import Flask, render_template, request, flash, redirect, url_for, session, send_file
import io
import re
import datetime

app = Flask(__name__)
app.secret_key = 'secret_key_for_flash_messages'




def generate_report_data(start_date, end_date, importer=None):
    # Convert the dates into YYYY-MM-DD format to make it compatible with SQL
    start_date_sql = pd.to_datetime(start_date).strftime('%Y-%m-%d')
    end_date_sql = pd.to_datetime(end_date).strftime('%Y-%m-%d')
    #Connect to SQLite database
    conn = sqlite3.connect('sales_management.db')

    # Query to fetch the data
    query = f'''
    SELECT
        S.quantity,
        s.sku as child_sku,
        P.item_type,
        P.sku as master_sku,
        P.product_name,
        P.importer,
        P.manufacturer,
        P.purchase_price,
        P.consumer_price,
        P.main_category,
        V.color,
        V.size,
        I.quantity as bin_quantity,
        B.manufacturer_sku
    FROM
        Sales S
        LEFT JOIN Variations V on S.sku = V.sku
        LEFT JOIN Products P on V.parent_sku = P.sku
        LEFT JOIN Inventory I on S.sku = I.sku
        LEFT JOIN BarcodesForVariations B on S.sku = B.sku
    WHERE
        date(substr(S.issue_date,7,4)||'-'||substr(S.issue_date,4,2)||'-'||substr(S.issue_date,1,2)) BETWEEN date('{start_date_sql}') AND date('{end_date_sql}')
'''

    # Adding condition for importer if it's not 'All'
    if importer != "All":
        query += f" AND P.importer = '{importer}' "

    query += '''
        ORDER BY
            S.quantity DESC
    '''

    # Fetch the data
    sales_report = pd.read_sql_query(query, conn)

    # Close the connection
    conn.close()
    # Fill NaN values with zeros
    sales_report = sales_report.fillna(0)

    # Group by 'child_sku', 'item_type' and other columns, and sum up 'quantity'
    grouped_report = sales_report.groupby(['child_sku', 'item_type', 'master_sku', 'manufacturer_sku', 'product_name', 'color', 'size', 'main_category', 'importer', 'manufacturer', 'purchase_price', 'consumer_price'], as_index=False).agg({'quantity':'sum', 'bin_quantity':'first'})

    # Filter rows where 'item_type' is 'רגיל' and group again
    grouped_regular = sales_report[sales_report['item_type'] == 'רגיל'].groupby(['child_sku', 'master_sku', 'manufacturer_sku', 'product_name', 'color', 'size', 'main_category', 'importer', 'manufacturer', 'purchase_price', 'consumer_price'], as_index=False).agg({'quantity':'sum', 'bin_quantity':'first'})

    # Concatenate the two dataframes
    final_report = pd.concat([grouped_report, grouped_regular], ignore_index=True)

    # Reorder the columns
    final_report = final_report[['child_sku', 'master_sku', 'manufacturer_sku', 'product_name', 'color', 'size', 'main_category', 'importer', 'manufacturer', 'quantity', 'bin_quantity']]

    # Sort by quantity (big to small), bin_quantity (big to small), product_name (alphabetical order), and importer (alphabetical order)
    final_report = final_report.sort_values(by=['quantity', 'bin_quantity', 'product_name', 'importer'], ascending=[False, False, True, True])

    #rename columns:
    final_report = final_report.rename(columns={'quantity': 'sold quantity', 'bin_quantity': 'Inventory'})

    #drop duplicates:
    final_report = final_report.drop_duplicates()
    
    # Add the line here to replace '0' in manufacturer_sku with child_sku
    final_report['manufacturer_sku'] = final_report['manufacturer_sku'].astype(str).str.strip()
    final_report['manufacturer_sku'] = np.where(final_report['manufacturer_sku'] == '0', final_report['child_sku'], final_report['manufacturer_sku'])


    ## Get the list of unique importers
    unique_importers = ['All'] + final_report['importer'].unique().tolist()
    print(f"Generated query with importer {importer}: {query}")
    return final_report

def get_all_unique_importers():
    # Fetch all unique importers from your database
    conn = sqlite3.connect('sales_management.db')
    unique_importers_query = "SELECT DISTINCT importer FROM Products"
    unique_importers = pd.read_sql_query(unique_importers_query, conn)['importer'].tolist()
    conn.close()
    return unique_importers


def import_csv_to_db(file_content, table_name, columns_to_keep, columns_to_rename, skiprows=1):
    file_like_object = io.StringIO(file_content)
    df = pd.read_csv(file_like_object, skiprows=skiprows)
    print("CSV columns:", df.columns)
    missing_columns = [col for col in columns_to_keep if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing columns in CSV: {', '.join(missing_columns)}")

    df = df[columns_to_keep]
    df.rename(columns=columns_to_rename, inplace=True)
    conn = sqlite3.connect('sales_management.db')
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    conn.close()
    
def update_last_loaded_timestamp(table):
    conn = sqlite3.connect('sales_management.db')
    cursor = conn.cursor()

    # Get current datetime
    current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # Check if the table name already exists in the Timestamps table
    cursor.execute(f"SELECT COUNT(*) FROM Timestamps WHERE table_name = '{table}'")
    exists = cursor.fetchone()[0]

    if exists:
        # Update the timestamp
        cursor.execute(f"UPDATE Timestamps SET last_loaded = '{current_time}' WHERE table_name = '{table}'")
    else:
        # Insert a new record
        cursor.execute(f"INSERT INTO Timestamps (table_name, last_loaded) VALUES ('{table}', '{current_time}')")

    conn.commit()
    conn.close()
    
def get_last_loaded_timestamp(table):
    conn = sqlite3.connect('sales_management.db')
    cursor = conn.cursor()
    cursor.execute(f"SELECT last_loaded FROM Timestamps WHERE table_name = '{table}'")
    result = cursor.fetchone()
    conn.close()
    if result:
        return result[0]
    return None

def run_matching_script():
    # Connect to the SQLite database
    conn = sqlite3.connect('sales_management.db')

    # Load data from Sales, Inventory, Colors, and Sizes tables
    sales_data = pd.read_sql_query("SELECT sku FROM Sales", conn)
    inventory_data = pd.read_sql_query("SELECT sku FROM Inventory", conn)
    colors_data = pd.read_sql_query("SELECT * FROM Colors", conn)
    sizes_data = pd.read_sql_query("SELECT * FROM Sizes", conn)

    # Combine SKU values from Sales and Inventory tables, and remove duplicates
    variation_sku_data = pd.concat([sales_data, inventory_data]).drop_duplicates()

    # Generate parent SKUs
    variation_sku_data['parent_sku'] = variation_sku_data['sku'].apply(lambda x: re.match('^[a-zA-Z0-9]*', str(x)).group())

    # Standardize data in colors and sizes tables by converting all text to lowercase
    colors_data = colors_data.applymap(lambda s:s.lower() if type(s) == str else s)
    sizes_data = sizes_data.applymap(lambda s:s.lower() if type(s) == str else s)
    
    # Order colors_data and sizes_data from longest to shortest string length for better matching
    colors_data = colors_data.sort_values(by='color_name', key=lambda x: -x.str.len()).reset_index(drop=True)
    sizes_data = sizes_data.sort_values(by='size_name', key=lambda x: -x.str.len()).reset_index(drop=True)

    def match_color_size(sku, parent_sku):
        color = None
        size = None
        
        # Convert SKU to string just to be safe
        sku = str(sku)

        # Extract the substring after the first occurrence of a non-alphanumeric character
        match_part = re.search(r'[^a-zA-Z0-9](.+)', sku)
        if match_part:
            sku_part = match_part.group(1)
        else:
            return color, size  # Return None for both color and size if no such character is found


        # If the parent_sku and variation_sku are not equal
        if parent_sku != sku:
            # Match color
            for index, row in colors_data.iterrows():
                if sku and any(substring in sku_part for substring in [str(row['Slug']), str(row['value'])] if substring):
                    color = row['color_name']
                    break

            # Match size
            for index, row in sizes_data.iterrows():
                if sku and any(substring in sku_part for substring in [str(row['Slug']), str(row['value'])] if substring):
                    size = row['size_name']
                    break

        return color, size
    # Apply the match_color_size function to each SKU to extract color and size
    variation_sku_data[['color', 'size']] = variation_sku_data.apply(lambda row: pd.Series(match_color_size(row['sku'], row['parent_sku'])), axis=1)
    # Save the updated 'Variations' table
    variation_sku_data.to_sql('Variations', conn, if_exists='replace', index=False)

    # Close the connection
    conn.close()

@app.route('/report', methods=['GET', 'POST'])
def report(*args, **kwargs):
    import datetime
    open('/tmp/test1', 'w').write(str(datetime.datetime.now()))
    return 'test123'
    data = None
    if request.method == 'POST':
        start_date = request.form.get('start_date')
        end_date = request.form.get('end_date')
        importer = request.form.get('importer') or 'All'
        
        # Store the dates and importer in the session
        session['start_date'] = start_date
        session['end_date'] = end_date
        session['importer'] = importer
        
        data = generate_report_data(start_date, end_date, importer)
    
    # This is executed on both POST and GET requests
    unique_importers = ['All'] + get_all_unique_importers()  # Define a function to fetch all unique importers
    return render_template('report.html', report=data.to_dict(orient='records') if data is not None else None, unique_importers=unique_importers)


@app.route('/show_report', methods=['GET', 'POST'])
def show_report():
    start_date = session.get('start_date')
    end_date = session.get('end_date')
    importer = session.get('importer')
    
    final_report = generate_report_data(start_date, end_date, importer)
    
    # Handle POST request when "Save CSV" button is clicked
    if request.method == 'POST':
        quantities_to_order = request.form.getlist('quantity_to_order')
        rows_with_quantity = final_report[final_report.index.isin([int(index) for index, qty in enumerate(quantities_to_order) if qty])]
        rows_with_quantity['Quantity to Order'] = [int(qty) for qty in quantities_to_order if qty]
        
        buf = io.BytesIO()
        desired_columns_for_csv = ['manufacturer_sku', 'product_name', 'color', 'size', 'manufacturer', 'Quantity to Order']
        rows_with_quantity[desired_columns_for_csv].to_csv(buf, index=False)
        buf.seek(0)

        response = send_file(buf, as_attachment=True, download_name='order_quantities.csv', mimetype='text/csv')
        response.headers["Content-Disposition"] = "attachment; filename=order_quantities.csv"
        return response
    
    # Filter columns for the view and add a placeholder column for Quantity to Order
    final_report['Quantity to Order'] = ''  # placeholder for input
    desired_columns = ['manufacturer_sku', 'product_name', 'color', 'size', 'manufacturer', 'sold quantity', 'Inventory', 'Quantity to Order']
    final_report = final_report[desired_columns]
    
    return render_template('report.html', report=final_report.to_dict(orient='records'))


@app.route('/download_csv', methods=['GET'])
def download_csv():
    start_date = session.get('start_date')
    end_date = session.get('end_date')
    importer = session.get('importer')
    
    final_report = generate_report_data(start_date, end_date, importer)

    buf = io.BytesIO()
    final_report.to_csv(buf, index=False)
    buf.seek(0)

    response = send_file(buf, as_attachment=True, download_name='report.csv', mimetype='text/csv')
    response.headers["Content-Disposition"] = "attachment; filename=report.csv"
    return response


@app.route('/run-matching', methods=['GET'])
def run_matching():
    try:
        run_matching_script()
        flash('Data processing was successful!')
    except Exception as e:
        flash(f"Error processing data: {str(e)}")
    return redirect('/')


@app.route('/', methods=['GET', 'POST'])
def choose_file():
    if request.method == 'POST':
        file = request.files['file']
        table_type = request.form.get('table_type')  # Get the type of table selected by the user

        if file.filename == '':
            flash('No selected file')
            return redirect(request.url)

        if file:
            file_content = file.stream.read().decode('utf-8')

            if table_type == "Products":
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
                import_csv_to_db(file_content, 'Products', catalog_columns, catalog_columns_rename, skiprows=1)
                update_last_loaded_timestamp('Products')
            elif table_type == "Sales":
                sales_columns = ["מקט", "שם פריט", "סוג מסמך", "מספר מסמך", "מזהה חשבון", "חברה", "מחיר פריט (לפני מעמ)", "כמות", "סך שורה לפני מעמ", "תאריך הפקה"]
                sales_columns_rename = {
                "מקט": "sku",
                "שם פריט": "product_name",
                "סוג מסמך": "document_type",
                "מספר מסמך": "document_number",
                "מזהה חשבון": "account_id",
                "חברה": "company",
                "מחיר פריט (לפני מעמ)": "item_price_before_tax",
                "כמות": "quantity",
                "סך שורה לפני מעמ": "total_line_before_tax",
                "תאריך הפקה": "issue_date"
            }
                import_csv_to_db(file_content, 'Sales', sales_columns, sales_columns_rename, skiprows=1)
                update_last_loaded_timestamp('Sales')

            elif table_type == "Inventory":
                inventory_columns = ["מקט", "מקט יצרן", "שם פריט", "כמות"]
                inventory_columns_rename = {
                "מקט": "sku",
                "מקט יצרן": "manufacturer_sku",
                "שם פריט": "item_name",
                "כמות": "quantity"
            }
                import_csv_to_db(file_content, 'Inventory', inventory_columns, inventory_columns_rename, skiprows=1)
                update_last_loaded_timestamp('Inventory')

            elif table_type == "Colors":
                colors_columns = ["name", "Slug", "value"]
                colors_columns_rename = {
                    "name": "color_name",
                    "Slug": "Slug",
                    "value": "value"
                }
                import_csv_to_db(file_content, 'Colors', colors_columns, colors_columns_rename, skiprows=0)
                update_last_loaded_timestamp('Colors')

            elif table_type == "Sizes":
                sizes_columns = ["name", "Slug", "value"]
                sizes_columns_rename = {
                    "name": "size_name",
                    "Slug": "Slug",
                    "value": "value"
                }
                import_csv_to_db(file_content, 'Sizes', sizes_columns, sizes_columns_rename, skiprows=0)
                update_last_loaded_timestamp('Sizes')

            elif table_type == "BarcodesForVariations":
                barcodes_columns = ["מקט", "מקט יצרן"]
                barcodes_columns_rename = {
                    "מקט": "sku",
                    "מקט יצרן": "manufacturer_sku"
                }
                import_csv_to_db(file_content, 'BarcodesForVariations', barcodes_columns, barcodes_columns_rename, skiprows=1)
                update_last_loaded_timestamp('BarcodesForVariations')

        # You can continue the structure for additional tables as needed.

            flash(f'{table_type} file successfully uploaded and processed')

    # Capture the current time
    current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    last_loaded_products = get_last_loaded_timestamp('Products')
    last_loaded_Sales = get_last_loaded_timestamp('Sales')
    last_loaded_Inventory = get_last_loaded_timestamp('Inventory')
    last_loaded_Colors = get_last_loaded_timestamp('Colors')
    last_loaded_Sizes = get_last_loaded_timestamp('Sizes')
    last_loaded_BarcodesForVariations = get_last_loaded_timestamp('BarcodesForVariations')
    
    return render_template('index.html', current_time=current_time, last_loaded_products=last_loaded_products, last_loaded_Sales=last_loaded_Sales, last_loaded_Inventory=last_loaded_Inventory, last_loaded_Colors=last_loaded_Colors, last_loaded_Sizes=last_loaded_Sizes, last_loaded_BarcodesForVariations=last_loaded_BarcodesForVariations)

if __name__ == "__main__":
    app.run(debug=True)
