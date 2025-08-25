import streamlit as st
import pandas as pd
import pyodbc
from datetime import datetime
import io
import numpy as np

# SQL Server Connection Details
SQL_SERVER = "HP-14Q\\TEW_SQLEXPRESS"
SQL_DATABASE = "OrderCheck"
SQL_USERNAME = "sa"
SQL_PASSWORD = "Mysql@2601"
SQL_DRIVER = "ODBC Driver 17 for SQL Server"

# Database connection function
def get_db_connection():
    try:
        conn_str = f"DRIVER={{{SQL_DRIVER}}};SERVER={SQL_SERVER};DATABASE={SQL_DATABASE};UID={SQL_USERNAME};PWD={SQL_PASSWORD}"
        conn = pyodbc.connect(conn_str)
        return conn
    except Exception as e:
        st.error(f"Database connection failed: {str(e)}")
        return None

# Initialize database tables
def init_db():
    conn = get_db_connection()
    if conn is None:
        return
    
    try:
        cursor = conn.cursor()
        
        # Create tables if they don't exist
        cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='orders_raw' AND xtype='U')
        CREATE TABLE orders_raw (
            id INT IDENTITY(1,1) PRIMARY KEY,
            scc VARCHAR(50),
            store_id VARCHAR(50),
            order_date DATE,
            material_code VARCHAR(50),
            material_description VARCHAR(255),
            uom VARCHAR(20),
            quantity DECIMAL(18,2),
            upload_timestamp DATETIME DEFAULT GETDATE(),
            processed BIT DEFAULT 0
        )
        """)
        
        cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='orders_processed' AND xtype='U')
        CREATE TABLE orders_processed (
            id INT IDENTITY(1,1) PRIMARY KEY,
            raw_order_id INT,
            scc VARCHAR(50),
            store_id VARCHAR(50),
            order_date DATE,
            material_code VARCHAR(50),
            material_description VARCHAR(255),
            uom VARCHAR(20),
            quantity DECIMAL(18,2),
            change_description VARCHAR(255),
            processed_timestamp DATETIME DEFAULT GETDATE(),
            FOREIGN KEY (raw_order_id) REFERENCES orders_raw(id)
        )
        """)
        
        cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='discontinued_materials' AND xtype='U')
        CREATE TABLE discontinued_materials (
            id INT IDENTITY(1,1) PRIMARY KEY,
            scc VARCHAR(50),
            discontinued_material VARCHAR(50),
            material_description_discon VARCHAR(255),
            replacement_material VARCHAR(50),
            material_description_repl VARCHAR(255),
            last_updated DATETIME DEFAULT GETDATE()
        )
        """)
        
        cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='average_quantities' AND xtype='U')
        CREATE TABLE average_quantities (
            id INT IDENTITY(1,1) PRIMARY KEY,
            scc VARCHAR(50),
            store_id VARCHAR(50),
            store_name VARCHAR(100),
            material_code VARCHAR(50),
            material_description VARCHAR(255),
            uom VARCHAR(20),
            avg_quantity DECIMAL(18,2),
            last_updated DATETIME DEFAULT GETDATE()
        )
        """)
        
        cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='operational_items' AND xtype='U')
        CREATE TABLE operational_items (
            id INT IDENTITY(1,1) PRIMARY KEY,
            material_code VARCHAR(50),
            material_description VARCHAR(255),
            last_updated DATETIME DEFAULT GETDATE()
        )
        """)
        
        conn.commit()
    except Exception as e:
        st.error(f"Database initialization failed: {str(e)}")
    finally:
        conn.close()

# Download template function
def download_template(template_type):
    if template_type == "order":
        df = pd.DataFrame(columns=["SCC", "Store ID", "Date", "Material code", 
                                  "Material description", "UoM", "Quantity"])
    elif template_type == "discontinued":
        df = pd.DataFrame(columns=["SCC", "Discontinued Material", "Material description_discon", 
                                  "Replacement material", "Material description_repl"])
    elif template_type == "average":
        df = pd.DataFrame(columns=["SCC", "Store ID", "Store name", "Material code", 
                                   "Material description", "UoM", "Avg Quantity"])
    elif template_type == "operational":
        df = pd.DataFrame(columns=["Material code", "Material description"])
    
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Template')
    output.seek(0)
    return output

# Check if order contains operational items and date is after 10th
def check_operational_items(order_date, material_codes):
    conn = get_db_connection()
    if conn is None:
        return False, []
    
    try:
        # Check if order date is after 10th of the month
        order_dt = pd.to_datetime(order_date)
        if order_dt.day <= 10:
            return False, []
        
        # Get operational items
        operational_df = pd.read_sql("SELECT * FROM operational_items", conn)
        if operational_df.empty:
            return False, []
        
        # Check if any material codes are operational items
        operational_in_order = []
        for code in material_codes:
            if code in operational_df['material_code'].values:
                desc = operational_df[operational_df['material_code'] == code]['material_description'].iloc[0]
                operational_in_order.append((code, desc))
        
        return len(operational_in_order) > 0, operational_in_order
        
    except Exception as e:
        st.error(f"Error checking operational items: {str(e)}")
        return False, []
    finally:
        conn.close()

# Process orders function
def process_orders(raw_df, skip_operational_items=False):
    conn = get_db_connection()
    if conn is None:
        return pd.DataFrame(), pd.DataFrame()
    
    try:
        # Get discontinued materials
        discontinued_df = pd.read_sql("SELECT * FROM discontinued_materials", conn)
        
        # Get average quantities
        avg_quantities_df = pd.read_sql("SELECT * FROM average_quantities", conn)
        
        # Get operational items if needed
        if skip_operational_items:
            operational_df = pd.read_sql("SELECT * FROM operational_items", conn)
            operational_codes = operational_df['material_code'].tolist()
        else:
            operational_codes = []
        
        processed_data = []
        change_log = []
        
        # Check for discontinued items that have their replacement already in the order
        discon_codes = discontinued_df['discontinued_material'].tolist()
        replacement_codes = discontinued_df['replacement_material'].tolist()
        
        # Create a mapping of discontinued items to their replacements
        discon_to_repl = {}
        for _, row in discontinued_df.iterrows():
            discon_to_repl[row['discontinued_material']] = row['replacement_material']
        
        # Get all material codes in the order
        order_material_codes = raw_df['Material code'].tolist()
        
        for _, row in raw_df.iterrows():
            scc = row['SCC']
            store_id = row['Store ID']
            material_code = row['Material code']
            uom = row['UoM']
            quantity = row['Quantity']
            
            # Skip operational items if requested
            if skip_operational_items and material_code in operational_codes:
                continue
                
            original_row = row.copy()
            change_description = None
            
            # Check if this material is discontinued and its replacement is already in the order
            if material_code in discon_codes and discon_to_repl[material_code] in order_material_codes:
                change_description = "Discontinued material removed (replacement already in order)"
                continue  # Skip adding this row to processed data
            
            # Check for discontinued materials
            discontinued_match = discontinued_df[
                (discontinued_df['scc'] == scc) & 
                (discontinued_df['discontinued_material'] == material_code)
            ]
            
            if not discontinued_match.empty:
                replacement = discontinued_match.iloc[0]['replacement_material']
                repl_desc = discontinued_match.iloc[0]['material_description_repl']
                
                if replacement != 'NA':
                    row['Material code'] = replacement
                    row['Material description'] = repl_desc
                    change_description = f"Discontinued material replaced with {replacement}"
                else:
                    change_description = "Discontinued material removed (no replacement)"
                    continue  # Skip adding this row to processed data
            
            # Check for quantity anomalies
            avg_match = avg_quantities_df[
                (avg_quantities_df['scc'] == scc) & 
                (avg_quantities_df['store_id'] == store_id) & 
                (avg_quantities_df['material_code'] == row['Material code']) & 
                (avg_quantities_df['uom'] == uom)
            ]
            
            if not avg_match.empty:
                avg_qty = avg_match.iloc[0]['avg_quantity']
                if quantity > 1.5 * avg_qty:
                    original_qty = row['Quantity']
                    row['Quantity'] = avg_qty
                    if change_description:
                        change_description += f"; Quantity adjusted from {original_qty} to {avg_qty}"
                    else:
                        change_description = f"Quantity adjusted from {original_qty} to {avg_qty}"
            
            # Add to processed data if not discontinued with no replacement
            processed_row = {
                'SCC': row['SCC'],
                'Store ID': row['Store ID'],
                'Date': row['Date'],
                'Material code': row['Material code'],
                'Material description': row['Material description'],
                'UoM': row['UoM'],
                'Quantity': row['Quantity'],
                'Change description': change_description or "No changes",
                'Original Material': original_row['Material code'],
                'Original Quantity': original_row['Quantity']
            }
            
            processed_data.append(processed_row)
            
            # For change log comparison
            if change_description and "No changes" not in change_description:
                change_log.append({
                    'Store ID': store_id,
                    'Original Material': original_row['Material code'],
                    'New Material': row['Material code'],
                    'Original Qty': original_row['Quantity'],
                    'New Qty': row['Quantity'],
                    'Change Reason': change_description
                })
        
        processed_df = pd.DataFrame(processed_data)
        change_log_df = pd.DataFrame(change_log) if change_log else pd.DataFrame()
        return processed_df, change_log_df
    
    except Exception as e:
        st.error(f"Error processing orders: {str(e)}")
        return pd.DataFrame(), pd.DataFrame()
    finally:
        conn.close()

# Save to database function
def save_to_db(df, table_name):
    conn = get_db_connection()
    if conn is None:
        return
    
    try:
        cursor = conn.cursor()
        
        # Prepare data for insertion
        data = df.to_dict('records')
        
        for row in data:
            if table_name == 'orders_raw':
                cursor.execute("""
                INSERT INTO orders_raw (scc, store_id, order_date, material_code, 
                                      material_description, uom, quantity)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """, 
                row['SCC'], row['Store ID'], row['Date'], row['Material code'],
                row['Material description'], row['UoM'], float(row['Quantity']))
            elif table_name == 'discontinued_materials':
                cursor.execute("""
                INSERT INTO discontinued_materials (scc, discontinued_material, 
                                                material_description_discon, 
                                                replacement_material, 
                                                material_description_repl)
                VALUES (?, ?, ?, ?, ?)
                """, 
                row['SCC'], row['Discontinued Material'], row['Material description_discon'],
                row['Replacement material'], row['Material description_repl'])
            elif table_name == 'average_quantities':
                cursor.execute("""
                INSERT INTO average_quantities (scc, store_id, store_name, material_code,
                                             material_description, uom, avg_quantity)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """, 
                row['SCC'], row['Store ID'], row['Store name'], row['Material code'],
                row['Material description'], row['UoM'], float(row['Avg Quantity']))
            elif table_name == 'operational_items':
                cursor.execute("""
                INSERT INTO operational_items (material_code, material_description)
                VALUES (?, ?)
                """, 
                row['Material code'], row['Material description'])
        
        conn.commit()
        st.success("Data saved to database successfully!")
    except Exception as e:
        st.error(f"Error saving to database: {str(e)}")
    finally:
        conn.close()

# View data from database
def view_data(table_name):
    conn = get_db_connection()
    if conn is None:
        return pd.DataFrame()
    
    try:
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql(query, conn)
        return df
    except Exception as e:
        st.error(f"Error viewing data: {str(e)}")
        return pd.DataFrame()
    finally:
        conn.close()

# Process and save orders with operational items handling
def process_and_save_orders(orders_to_process, selected_scc, selected_date, skip_operational):
    conn = get_db_connection()
    if conn is None:
        return
    
    try:
        # Create a mapping dictionary for original order IDs
        original_id_map = {}
        for _, row in orders_to_process.iterrows():
            key = (row['scc'], row['store_id'], row['order_date'], row['material_code'])
            original_id_map[key] = row['id']
        
        # Rename columns to match expected format
        orders_to_process = orders_to_process.rename(columns={
            'scc': 'SCC',
            'store_id': 'Store ID',
            'order_date': 'Date',
            'material_code': 'Material code',
            'material_description': 'Material description',
            'uom': 'UoM',
            'quantity': 'Quantity'
        })
        
        processed_df, change_log_df = process_orders(orders_to_process, skip_operational)
        
        st.subheader("Processed Orders")
        st.dataframe(processed_df)
        
        if not change_log_df.empty:
            st.subheader("Changes Applied")
            st.dataframe(change_log_df)
        
        # Save processed orders to database
        cursor = conn.cursor()
        
        # Mark original orders as processed
        order_ids = orders_to_process['id'].astype(int).tolist()
        for order_id in order_ids:
            cursor.execute("""
            UPDATE orders_raw SET processed = 1 WHERE id = ?
            """, int(order_id))
        
        # Save processed orders
        for _, row in processed_df.iterrows():
            # Get the original ID from our mapping
            key = (row['SCC'], row['Store ID'], row['Date'], row['Original Material'])
            original_id = original_id_map.get(key)
            
            if original_id is not None:
                cursor.execute("""
                INSERT INTO orders_processed (raw_order_id, scc, store_id, order_date, 
                                            material_code, material_description, 
                                            uom, quantity, change_description)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, 
                int(original_id),
                row['SCC'], 
                row['Store ID'], 
                row['Date'], 
                row['Material code'], 
                row['Material description'], 
                row['UoM'], 
                float(row['Quantity']), 
                row['Change description'])
        
        conn.commit()
        st.success(f"Successfully processed {len(processed_df)} orders!")
        
        # Export option
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            processed_df.to_excel(writer, index=False, sheet_name='Processed Orders')
            if not change_log_df.empty:
                change_log_df.to_excel(writer, index=False, sheet_name='Change Log')
        output.seek(0)
        
        st.download_button(
            label="Download Processed Orders",
            data=output,
            file_name=f"processed_orders_{selected_scc}_{selected_date}.xlsx",
            mime="application/vnd.ms-excel"
        )
    except Exception as e:
        st.error(f"Error processing orders: {str(e)}")
    finally:
        conn.close()

# Main app function
def main():
    st.set_page_config(page_title="Order Hygiene Portal", layout="wide")
    
    # Initialize database
    init_db()
    
    st.title("Order Hygiene Check Portal")
    st.write("Clean and validate store orders automatically")
    
    # Sidebar navigation
    st.sidebar.title("Navigation")
    app_mode = st.sidebar.radio("Go to", 
                               ["Upload Orders", "Manage Masters", 
                                "Process Orders", "View Data", "Reports"])
    
    if app_mode == "Upload Orders":
        st.header("Upload New Orders")
        
        uploaded_file = st.file_uploader("Upload Order Excel File", type=["xlsx", "xls"])
        
        col1, col2 = st.columns(2)
        with col1:
            st.download_button(
                label="Download Order Template",
                data=download_template("order"),
                file_name="order_template.xlsx",
                mime="application/vnd.ms-excel"
            )
        
        if uploaded_file:
            try:
                df = pd.read_excel(uploaded_file)
                required_cols = ["SCC", "Store ID", "Date", "Material code", 
                                "Material description", "UoM", "Quantity"]
                
                if all(col in df.columns for col in required_cols):
                    st.success("File uploaded successfully!")
                    st.dataframe(df.head())
                    
                    if st.button("Save to Database"):
                        save_to_db(df, 'orders_raw')
                else:
                    st.error(f"Missing required columns. File must contain: {', '.join(required_cols)}")
            except Exception as e:
                st.error(f"Error reading file: {str(e)}")
        
        # View uploaded orders
        st.subheader("View Uploaded Orders")
        if st.button("Refresh Orders Data"):
            orders_df = view_data("orders_raw")
            if not orders_df.empty:
                st.dataframe(orders_df)
            else:
                st.info("No orders found in database.")
    
    elif app_mode == "Manage Masters":
        st.header("Manage Master Data")
        
        master_type = st.radio("Select Master Type", 
                             ["Discontinued Materials", "Average Quantities", "Operational Items"])
        
        if master_type == "Discontinued Materials":
            st.subheader("Discontinued Materials Master")
            
            col1, col2 = st.columns(2)
            with col1:
                st.download_button(
                    label="Download Template",
                    data=download_template("discontinued"),
                    file_name="discontinued_materials_template.xlsx",
                    mime="application/vnd.ms-excel"
                )
            
            uploaded_file = st.file_uploader("Upload Discontinued Materials", type=["xlsx", "xls"])
            
            if uploaded_file:
                try:
                    df = pd.read_excel(uploaded_file)
                    required_cols = ["SCC", "Discontinued Material", "Material description_discon", 
                                   "Replacement material", "Material description_repl"]
                    
                    if all(col in df.columns for col in required_cols):
                        st.success("File uploaded successfully!")
                        st.dataframe(df.head())
                        
                        if st.button("Update Discontinued Materials"):
                            # Clear old data
                            conn = get_db_connection()
                            if conn:
                                cursor = conn.cursor()
                                cursor.execute("DELETE FROM discontinued_materials")
                                conn.commit()
                                conn.close()
                            
                            # Save new data
                            save_to_db(df, 'discontinued_materials')
                    else:
                        st.error(f"Missing required columns. File must contain: {', '.join(required_cols)}")
                except Exception as e:
                    st.error(f"Error reading file: {str(e)}")
            
            # Manual entry
            st.subheader("Manual Entry")
            with st.form("discontinued_form"):
                scc = st.text_input("SCC")
                discon_mat = st.text_input("Discontinued Material")
                discon_desc = st.text_input("Material Description (Discontinued)")
                repl_mat = st.text_input("Replacement Material (NA if no replacement)")
                repl_desc = st.text_input("Material Description (Replacement)")
                
                submitted = st.form_submit_button("Add Entry")
                if submitted:
                    conn = get_db_connection()
                    if conn:
                        cursor = conn.cursor()
                        cursor.execute("""
                        INSERT INTO discontinued_materials (scc, discontinued_material, 
                                                        material_description_discon, 
                                                        replacement_material, 
                                                        material_description_repl)
                        VALUES (?, ?, ?, ?, ?)
                        """, scc, discon_mat, discon_desc, repl_mat, repl_desc)
                        conn.commit()
                        conn.close()
                        st.success("Entry added successfully!")
            
            # View discontinued materials
            st.subheader("View Discontinued Materials")
            if st.button("Refresh Discontinued Materials"):
                discontinued_df = view_data("discontinued_materials")
                if not discontinued_df.empty:
                    st.dataframe(discontinued_df)
                else:
                    st.info("No discontinued materials found in database.")
        
        elif master_type == "Average Quantities":
            st.subheader("Average Quantities Master")
            
            col1, col2 = st.columns(2)
            with col1:
                st.download_button(
                    label="Download Template",
                    data=download_template("average"),
                    file_name="average_quantities_template.xlsx",
                    mime="application/vnd.ms-excel"
                )
            
            uploaded_file = st.file_uploader("Upload Average Quantities", type=["xlsx", "xls"])
            
            if uploaded_file:
                try:
                    df = pd.read_excel(uploaded_file)
                    required_cols = ["SCC", "Store ID", "Store name", "Material code", 
                                    "Material description", "UoM", "Avg Quantity"]
                    
                    if all(col in df.columns for col in required_cols):
                        st.success("File uploaded successfully!")
                        st.dataframe(df.head())
                        
                        if st.button("Update Average Quantities"):
                            # Clear old data
                            conn = get_db_connection()
                            if conn:
                                cursor = conn.cursor()
                                cursor.execute("DELETE FROM average_quantities")
                                conn.commit()
                                conn.close()
                            
                            # Save new data
                            save_to_db(df, 'average_quantities')
                    else:
                        st.error(f"Missing required columns. File must contain: {', '.join(required_cols)}")
                except Exception as e:
                    st.error(f"Error reading file: {str(e)}")
            
            # Manual entry
            st.subheader("Manual Entry")
            with st.form("average_form"):
                scc = st.text_input("SCC")
                store_id = st.text_input("Store ID")
                store_name = st.text_input("Store Name")
                mat_code = st.text_input("Material Code")
                mat_desc = st.text_input("Material Description")
                uom = st.text_input("UoM")
                avg_qty = st.number_input("Average Quantity", min_value=0.0)
                
                submitted = st.form_submit_button("Add Entry")
                if submitted:
                    conn = get_db_connection()
                    if conn:
                        cursor = conn.cursor()
                        cursor.execute("""
                        INSERT INTO average_quantities (scc, store_id, store_name, material_code,
                                                     material_description, uom, avg_quantity)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        """, scc, store_id, store_name, mat_code, mat_desc, uom, float(avg_qty))
                        conn.commit()
                        conn.close()
                        st.success("Entry added successfully!")
            
            # View average quantities
            st.subheader("View Average Quantities")
            if st.button("Refresh Average Quantities"):
                avg_quantities_df = view_data("average_quantities")
                if not avg_quantities_df.empty:
                    st.dataframe(avg_quantities_df)
                else:
                    st.info("No average quantities found in database.")
        
        elif master_type == "Operational Items":
            st.subheader("Operational Items Master")
            
            col1, col2 = st.columns(2)
            with col1:
                st.download_button(
                    label="Download Template",
                    data=download_template("operational"),
                    file_name="operational_items_template.xlsx",
                    mime="application/vnd.ms-excel"
                )
            
            uploaded_file = st.file_uploader("Upload Operational Items", type=["xlsx", "xls"])
            
            if uploaded_file:
                try:
                    df = pd.read_excel(uploaded_file)
                    required_cols = ["Material code", "Material description"]
                    
                    if all(col in df.columns for col in required_cols):
                        st.success("File uploaded successfully!")
                        st.dataframe(df.head())
                        
                        if st.button("Update Operational Items"):
                            # Clear old data
                            conn = get_db_connection()
                            if conn:
                                cursor = conn.cursor()
                                cursor.execute("DELETE FROM operational_items")
                                conn.commit()
                                conn.close()
                            
                            # Save new data
                            save_to_db(df, 'operational_items')
                    else:
                        st.error(f"Missing required columns. File must contain: {', '.join(required_cols)}")
                except Exception as e:
                    st.error(f"Error reading file: {str(e)}")
            
            # Manual entry
            st.subheader("Manual Entry")
            with st.form("operational_form"):
                mat_code = st.text_input("Material Code")
                mat_desc = st.text_input("Material Description")
                
                submitted = st.form_submit_button("Add Entry")
                if submitted:
                    conn = get_db_connection()
                    if conn:
                        cursor = conn.cursor()
                        cursor.execute("""
                        INSERT INTO operational_items (material_code, material_description)
                        VALUES (?, ?)
                        """, mat_code, mat_desc)
                        conn.commit()
                        conn.close()
                        st.success("Entry added successfully!")
            
            # View operational items
            st.subheader("View Operational Items")
            if st.button("Refresh Operational Items"):
                operational_df = view_data("operational_items")
                if not operational_df.empty:
                    st.dataframe(operational_df)
                else:
                    st.info("No operational items found in database.")
    
    elif app_mode == "Process Orders":
        st.header("Process Orders")
        
        # Initialize session state for operational items handling
        if 'operational_confirmation' not in st.session_state:
            st.session_state.operational_confirmation = None
        if 'show_confirmation' not in st.session_state:
            st.session_state.show_confirmation = False
        
        conn = get_db_connection()
        if conn is None:
            return
        
        try:
            # Get unprocessed orders grouped by SCC and date
            unprocessed_orders = pd.read_sql("""
            SELECT 
                scc,
                order_date,
                COUNT(*) as order_count
            FROM orders_raw 
            WHERE processed = 0
            GROUP BY scc, order_date
            ORDER BY scc, order_date
            """, conn)
            
            if unprocessed_orders.empty:
                st.info("No unprocessed orders found.")
            else:
                st.subheader("Unprocessed Orders Summary")
                st.dataframe(unprocessed_orders)
                
                # Create selection options
                scc_options = unprocessed_orders['scc'].unique()
                selected_scc = st.selectbox("Select SCC", options=scc_options)
                
                # Filter dates for selected SCC
                scc_dates = unprocessed_orders[unprocessed_orders['scc'] == selected_scc]['order_date'].unique()
                selected_date = st.selectbox("Select Order Date", options=scc_dates)
                
                if st.button("Load Selected Orders"):
                    # Get all unprocessed orders for selected SCC and date
                    orders_to_process = pd.read_sql(f"""
                    SELECT * FROM orders_raw 
                    WHERE processed = 0 
                    AND scc = '{selected_scc}'
                    AND order_date = '{selected_date}'
                    """, conn)
                    
                    if orders_to_process.empty:
                        st.warning("No orders found for the selected criteria.")
                    else:
                        st.session_state.orders_to_process = orders_to_process
                        st.session_state.selected_scc = selected_scc
                        st.session_state.selected_date = selected_date
                        
                        # Check for operational items
                        material_codes = orders_to_process['material_code'].tolist()
                        has_operational, operational_items = check_operational_items(selected_date, material_codes)
                        
                        if has_operational:
                            st.warning("Operational items detected in this order!")
                            st.write("The following operational items were found:")
                            
                            operational_df = pd.DataFrame(operational_items, columns=['Material Code', 'Material Description'])
                            st.dataframe(operational_df)
                            
                            # Store operational items in session state
                            st.session_state.has_operational = True
                            st.session_state.operational_items = operational_items
                            st.session_state.show_confirmation = True
                            
                        else:
                            st.session_state.has_operational = False
                            st.session_state.show_confirmation = False
                            # Process orders directly if no operational items
                            process_and_save_orders(orders_to_process, selected_scc, selected_date, False)
                
                # Show confirmation options if operational items were detected
                if st.session_state.get('show_confirmation', False) and st.session_state.get('has_operational', False):
                    st.subheader("Operational Items Confirmation")
                    
                    # Use session state to persist the selection
                    if st.session_state.operational_confirmation is None:
                        st.session_state.operational_confirmation = "Yes, process all items"
                    
                    operational_choice = st.radio(
                        "Do you want to process these operational items?",
                        ["Yes, process all items", "No, skip operational items"],
                        key="operational_choice"
                    )
                    
                    # Update session state with the selection
                    st.session_state.operational_confirmation = operational_choice
                    
                    if st.button("Confirm and Process"):
                        skip_operational = st.session_state.operational_confirmation == "No, skip operational items"
                        process_and_save_orders(
                            st.session_state.orders_to_process, 
                            st.session_state.selected_scc, 
                            st.session_state.selected_date, 
                            skip_operational
                        )
                        # Reset the confirmation state
                        st.session_state.show_confirmation = False
                        st.session_state.operational_confirmation = None
        
        finally:
            conn.close()
    
    elif app_mode == "View Data":
        st.header("View Data")
        
        data_type = st.radio("Select Data Type", 
                           ["Raw Orders", "Processed Orders", "Discontinued Materials", "Average Quantities", "Operational Items"])
        
        if data_type == "Raw Orders":
            st.subheader("Raw Orders Data")
            raw_orders_df = view_data("orders_raw")
            if not raw_orders_df.empty:
                st.dataframe(raw_orders_df)
            else:
                st.info("No raw orders found in database.")
        
        elif data_type == "Processed Orders":
            st.subheader("Processed Orders Data")
            processed_orders_df = view_data("orders_processed")
            if not processed_orders_df.empty:
                st.dataframe(processed_orders_df)
            else:
                st.info("No processed orders found in database.")
        
        elif data_type == "Discontinued Materials":
            st.subheader("Discontinued Materials Data")
            discontinued_df = view_data("discontinued_materials")
            if not discontinued_df.empty:
                st.dataframe(discontinued_df)
            else:
                st.info("No discontinued materials found in database.")
        
        elif data_type == "Average Quantities":
            st.subheader("Average Quantities Data")
            avg_quantities_df = view_data("average_quantities")
            if not avg_quantities_df.empty:
                st.dataframe(avg_quantities_df)
            else:
                st.info("No average quantities found in database.")
                
        elif data_type == "Operational Items":
            st.subheader("Operational Items Data")
            operational_df = view_data("operational_items")
            if not operational_df.empty:
                st.dataframe(operational_df)
            else:
                st.info("No operational items found in database.")
    
    elif app_mode == "Reports":
        st.header("Reports and Analytics")
        
        conn = get_db_connection()
        if conn is None:
            return
        
        try:
            report_type = st.selectbox(
                "Select Report Type",
                ["Discrepancy Analysis", "Most Changed Items", "Store Compliance", "Operational Items Report"]
            )
            
            if report_type == "Discrepancy Analysis":
                st.subheader("Discrepancy Analysis")
                
                # Get changes data
                changes_df = pd.read_sql("""
                SELECT 
                    p.scc,
                    p.store_id,
                    p.material_code,
                    p.material_description,
                    p.change_description,
                    COUNT(*) as change_count
                FROM orders_processed p
                WHERE p.change_description != 'No changes'
                GROUP BY p.scc, p.store_id, p.material_code, p.material_description, p.change_description
                ORDER BY change_count DESC
                """, conn)
                
                if changes_df.empty:
                    st.info("No discrepancies found in processed orders.")
                else:
                    st.dataframe(changes_df)
                    
                    # Visualization
                    st.bar_chart(changes_df.head(10).set_index('material_description')['change_count'])
            
            elif report_type == "Most Changed Items":
                st.subheader("Most Frequently Changed Items")
                
                items_df = pd.read_sql("""
                SELECT 
                    material_code,
                    material_description,
                    COUNT(*) as total_changes
                FROM orders_processed
                WHERE change_description != 'No changes'
                GROUP BY material_code, material_description
                ORDER BY total_changes DESC
                """, conn)
                
                if items_df.empty:
                    st.info("No changed items found.")
                else:
                    st.dataframe(items_df)
                    
                    # Top 10 chart
                    st.subheader("Top 10 Most Changed Items")
                    st.bar_chart(items_df.head(10).set_index('material_description')['total_changes'])
            
            elif report_type == "Store Compliance":
                st.subheader("Store Compliance Report")
                
                compliance_df = pd.read_sql("""
                SELECT 
                    p.scc,
                    p.store_id,
                    COUNT(*) as total_orders,
                    SUM(CASE WHEN p.change_description = 'No changes' THEN 1 ELSE 0 END) as compliant_orders,
                    SUM(CASE WHEN p.change_description != 'No changes' THEN 1 ELSE 0 END) as non_compliant_orders,
                    CAST(SUM(CASE WHEN p.change_description = 'No changes' THEN 1 ELSE 0 END) AS FLOAT) / COUNT(*) * 100 as compliance_rate
                FROM orders_processed p
                GROUP BY p.scc, p.store_id
                ORDER BY compliance_rate DESC
                """, conn)
                
                if compliance_df.empty:
                    st.info("No compliance data available.")
                else:
                    st.dataframe(compliance_df)
                    
                    # Compliance rate visualization
                    st.subheader("Compliance Rate by Store")
                    st.bar_chart(compliance_df.set_index('store_id')['compliance_rate'])
            
            elif report_type == "Operational Items Report":
                st.subheader("Operational Items Usage Report")
                
                operational_df = pd.read_sql("""
                SELECT 
                    o.material_code,
                    o.material_description,
                    COUNT(DISTINCT p.order_date) as order_days_count,
                    COUNT(p.id) as total_orders,
                    MIN(p.order_date) as first_ordered,
                    MAX(p.order_date) as last_ordered
                FROM operational_items o
                LEFT JOIN orders_processed p ON o.material_code = p.material_code
                GROUP BY o.material_code, o.material_description
                ORDER BY total_orders DESC
                """, conn)
                
                if operational_df.empty:
                    st.info("No operational items data available.")
                else:
                    st.dataframe(operational_df)
                    
                    # Operational items usage chart
                    st.subheader("Operational Items Order Frequency")
                    chart_data = operational_df[['material_description', 'total_orders']].set_index('material_description')
                    st.bar_chart(chart_data.head(10))
        finally:
            conn.close()

if __name__ == "__main__":
    main()