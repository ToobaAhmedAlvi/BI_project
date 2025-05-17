import os
import pandas as pd
from sqlalchemy import create_engine

# Database connection parameters
server_name = 'DESKTOP-NU99729\\SQLEXPRESS' 
database_name = 'chinook_db'

# Create the connection string
engine = create_engine(
    f'mssql+pyodbc://{server_name}/{database_name}?driver=ODBC+Driver+17+for+SQL+Server'
)

# Folder location with CSV files
folder_path = R'E:\IBA_MS_DS 2026\Chinook_db'

def upload_csv_to_sql(folder_path, engine):
    processed_files = []  # Track processed files
    
    for file_name in os.listdir(folder_path):
        if file_name.endswith('.csv'):
            file_path = os.path.join(folder_path, file_name)
            print(f'Processing file: {file_path}')
            
            df = pd.read_csv(file_path)  # Read CSV file
            table_name = os.path.splitext(file_name)[0]  # Remove ".csv" from the filename

            df.to_sql(table_name, con=engine, if_exists='replace', index=False)
            processed_files.append(table_name)
            print(f'CSV file {file_name} uploaded to table {table_name}.')

    if processed_files:
        print("\n✅ All files have been successfully uploaded to the database!")
        print("Uploaded tables:", ', '.join(processed_files))
    else:
        print("\n⚠️ No CSV files found in the folder.")

# Call the function
upload_csv_to_sql(folder_path, engine)
