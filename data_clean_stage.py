import os
import pathlib
import pandas as pd
from snowflake.snowpark import Session
from dotenv import load_dotenv

load_dotenv()

# Snowflake connection parameters from env variables
snowflake_params = {
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "role": os.getenv("SNOWFLAKE_ROLE"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "database": os.getenv("SNOWFLAKE_DATABASE"),
    "schema": os.getenv("SNOWFLAKE_SCHEMA"),  # Raw schema for stage
}

new_database = snowflake_params["database"]
raw_schema = snowflake_params["schema"]
cleaned_schema = "ERD_SCHEMA_CLEANED"
cleaning_stage = "DATA_CLEANING_STAGE"

# Reusable location lookup dictionary
location_lookup = {
    ("Stuttgart", "Germany"): {"State": "Baden-Württemberg", "PostalCode": "70174"},
    ("Oslo", "Norway"): {"State": "Oslo", "PostalCode": "0171"},
    ("Prague", "Czech Republic"): {"State": "Prague", "PostalCode": "14700"},
    ("Vienna", "Austria"): {"State": "Vienna", "PostalCode": "1010"},
    ("Brussels", "Belgium"): {"State": "Brussels-Capital", "PostalCode": "1000"},
    ("Copenhagen", "Denmark"): {"State": "Hovedstaden", "PostalCode": "1720"},
    ("Lisbon", "Portugal"): {"State": "Lisbon", "PostalCode": "1100-042"},
    ("Porto", "Portugal"): {"State": "Porto", "PostalCode": "4350-414"},
    ("Berlin", "Germany"): {"State": "Berlin", "PostalCode": "10789"},
    ("Frankfurt", "Germany"): {"State": "Hesse", "PostalCode": "60316"},
    ("Paris", "France"): {"State": "Île-de-France", "PostalCode": "75009"},
    ("Lyon", "France"): {"State": "Auvergne-Rhône-Alpes", "PostalCode": "69002"},
    ("Bordeaux", "France"): {"State": "Nouvelle-Aquitaine", "PostalCode": "33000"},
    ("Dijon", "France"): {"State": "Bourgogne-Franche-Comté", "PostalCode": "21000"},
    ("Helsinki", "Finland"): {"State": "Uusimaa", "PostalCode": "00530"},
    ("Budapest", "Hungary"): {"State": "Budapest", "PostalCode": "1073"},
    ("Warsaw", "Poland"): {"State": "Mazowieckie", "PostalCode": "00-358"},
    ("Madrid", "Spain"): {"State": "Community of Madrid", "PostalCode": "28015"},
    ("Stockholm", "Sweden"): {"State": "SC", "PostalCode": "11230"},
    ("London", "United Kingdom"): {"State": "England", "PostalCode": "N1 5LH"},
    ("Edinburgh", "United Kingdom"): {"State": "Scotland", "PostalCode": "EH4 1HH"},
    ("Buenos Aires", "Argentina"): {"State": "BA", "PostalCode": "1106"},
    ("Santiago", "Chile"): {"State": "SA", "PostalCode": "8320000"},
    ("Dublin", "Ireland"): {"State": "Dublin", "PostalCode": "D02 A529"},
    ("Delhi", "India"): {"State": "Delhi", "PostalCode": "110017"},
    ("Bangalore", "India"): {"State": "Karnataka", "PostalCode": "560001"},
     ("Vienna", "Austria"): {"State": "Vienna", "PostalCode": "1010"}
}

# ==== Cleaning functions ====

def fill_state(city, country, current_state):
    key = (city.strip() if pd.notnull(city) else None, country.strip() if pd.notnull(country) else None)
    if not current_state or pd.isna(current_state) or str(current_state).strip() == "":
        return location_lookup.get(key, {}).get("State", current_state)
    return current_state

def fill_postalcode(city, country, current_postal):
    key = (city.strip() if pd.notnull(city) else None, country.strip() if pd.notnull(country) else None)
    if not current_postal or pd.isna(current_postal) or str(current_postal).strip() == "":
        return location_lookup.get(key, {}).get("PostalCode", current_postal)
    return current_postal

def clean_customer(df):
    df.columns = df.columns.str.strip().str.upper()
    df['STATE'] = df.apply(lambda row: fill_state(row['CITY'], row['COUNTRY'], row['STATE']), axis=1)
    df['POSTALCODE'] = df.apply(lambda row: fill_postalcode(row['CITY'], row['COUNTRY'], row['POSTALCODE']), axis=1)
    df.fillna({'FAX': '', 'COMPANY': '', 'EMAIL': ''}, inplace=True)
    df.drop_duplicates(inplace=True)
    df = df[df['CUSTOMERID'].notnull()]
    return df

def clean_invoice(df):
    df.columns = df.columns.str.strip().str.upper()
    df['INVOICEDATE'] = pd.to_datetime(df['INVOICEDATE'], errors='coerce')
    # Fill BILLINGSTATE and BILLINGPOSTALCODE similar to customer
    df['BILLINGSTATE'] = df.apply(lambda row: fill_state(row.get('BILLINGCITY', None), row.get('BILLINGCOUNTRY', None), row['BILLINGSTATE']), axis=1)
    df['BILLINGPOSTALCODE'] = df.apply(lambda row: fill_postalcode(row.get('BILLINGCITY', None), row.get('BILLINGCOUNTRY', None), row['BILLINGPOSTALCODE']), axis=1)
    df.fillna({'BILLINGADDRESS': '', 'BILLINGCITY': '', 'BILLINGSTATE': '', 'BILLINGCOUNTRY': '', 'BILLINGPOSTALCODE': ''}, inplace=True)
    df.drop_duplicates(inplace=True)
    return df

def clean_track(df):
    df.columns = df.columns.str.strip().str.upper()
    df['ALBUMID'] = pd.to_numeric(df['ALBUMID'], errors='coerce').fillna(0).astype(int)
    df['MEDIATYPEID'] = pd.to_numeric(df['MEDIATYPEID'], errors='coerce').fillna(0).astype(int)
    df['GENREID'] = pd.to_numeric(df['GENREID'], errors='coerce').fillna(0).astype(int)
    df['MILLISECONDS'] = pd.to_numeric(df['MILLISECONDS'], errors='coerce').fillna(0).astype(int)
    df['BYTES'] = pd.to_numeric(df['BYTES'], errors='coerce').fillna(0).astype(int)
    df['UNITPRICE'] = pd.to_numeric(df['UNITPRICE'], errors='coerce').fillna(0.0)
    # Replace null or empty COMPOSER with 'Anonymous'
    df['COMPOSER'] = df['COMPOSER'].fillna('').apply(lambda x: x if str(x).strip() != '' else 'Anonymous')
    df.drop_duplicates(inplace=True)
    return df

# The rest of cleaning functions unchanged

def clean_employee(df):
    df.columns = df.columns.str.strip().str.upper()
    df['BIRTHDATE'] = pd.to_datetime(df['BIRTHDATE'], errors='coerce')
    df['HIREDATE'] = pd.to_datetime(df['HIREDATE'], errors='coerce')
    df.fillna({'PHONE': '', 'FAX': ''}, inplace=True)
    df.drop_duplicates(inplace=True)
    return df

def clean_artist(df):
    df.columns = df.columns.str.strip().str.upper()
    df = df[df['NAME'].notnull()]
    df.drop_duplicates(inplace=True)
    return df

def clean_album(df):
    df.columns = df.columns.str.strip().str.upper()
    df['ARTISTID'] = pd.to_numeric(df['ARTISTID'], errors='coerce').fillna(0).astype(int)
    df.drop_duplicates(inplace=True)
    return df

def clean_invoiceline(df):
    df.columns = df.columns.str.strip().str.upper()
    df['UNITPRICE'] = pd.to_numeric(df['UNITPRICE'], errors='coerce').fillna(0.0)
    df['QUANTITY'] = pd.to_numeric(df['QUANTITY'], errors='coerce').fillna(0).astype(int)
    df.drop_duplicates(inplace=True)
    return df

def clean_genre(df):
    df.columns = df.columns.str.strip().str.upper()
    df = df[df['NAME'].notnull()]
    df.drop_duplicates(inplace=True)
    return df

def clean_mediatype(df):
    df.columns = df.columns.str.strip().str.upper()
    df = df[df['NAME'].notnull()]
    df.drop_duplicates(inplace=True)
    return df

def clean_playlist(df):
    df.columns = df.columns.str.strip().str.upper()
    df = df[df['NAME'].notnull()]
    df.drop_duplicates(inplace=True)
    return df

def clean_playlisttrack(df):
    df.columns = df.columns.str.strip().str.upper()
    df['PLAYLISTID'] = pd.to_numeric(df['PLAYLISTID'], errors='coerce').fillna(0).astype(int)
    df['TRACKID'] = pd.to_numeric(df['TRACKID'], errors='coerce').fillna(0).astype(int)
    df.drop_duplicates(inplace=True)
    return df

# ==== Snowflake interaction functions (unchanged) ====

def create_cleaning_stage(session):
    sql = f'''
        CREATE STAGE IF NOT EXISTS "{new_database}"."{raw_schema}"."{cleaning_stage}"
        FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '\"' SKIP_HEADER = 1)
    '''
    print("Creating cleaning stage if not exists...")
    session.sql(sql).collect()

def create_cleaned_schema(session):
    sql = f'CREATE SCHEMA IF NOT EXISTS "{new_database}"."{cleaned_schema}"'
    print("Creating cleaned schema if not exists...")
    session.sql(sql).collect()

def export_to_csv(df, table_name):
    filename = f"{table_name}_cleaned.csv"
    df.to_csv(filename, index=False)
    print(f"Exported cleaned data to {filename}")
    return filename

def remove_file_from_stage(session, filename):
    remove_sql = f"REMOVE @\"{new_database}\".\"{raw_schema}\".\"{cleaning_stage}\"/{filename}.gz"
    print(f"Removing old staged file {filename}.gz ...")
    session.sql(remove_sql).collect()

def upload_to_stage(session, csv_file):
    csv_path = pathlib.Path(csv_file).resolve().as_posix()
    filename = pathlib.Path(csv_file).name
    # Remove old staged file before upload to avoid PUT SKIPPED
    remove_file_from_stage(session, filename)
    put_sql = f"PUT 'file://{csv_path}' @\"{new_database}\".\"{raw_schema}\".\"{cleaning_stage}\" AUTO_COMPRESS=TRUE"
    print(f"Uploading {csv_path} to stage {new_database}.{raw_schema}.{cleaning_stage} ...")
    res = session.sql(put_sql).collect()
    print("PUT result:", res)

def copy_into_cleaned_table(session, table_name, csv_file):
    copy_sql = f'''
        COPY INTO "{cleaned_schema}"."{table_name.upper()}"
        FROM @\"{new_database}\".\"{raw_schema}\".\"{cleaning_stage}\"/{csv_file}.gz
        FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='\"' SKIP_HEADER=1)
        ON_ERROR = 'CONTINUE'
    '''
    print(f"Copying data into {cleaned_schema}.{table_name.upper()} from staged file {csv_file}.gz ...")
    res = session.sql(copy_sql).collect()
    print("COPY INTO result:", res)

def truncate_cleaned_table(session, table_name):
    truncate_sql = f'TRUNCATE TABLE "{cleaned_schema}"."{table_name.upper()}"'
    print(f"Truncating table {cleaned_schema}.{table_name.upper()} before loading fresh data...")
    session.sql(truncate_sql).collect()

def clean_data(df, table_name):
    switcher = {
        "Customer": clean_customer,
        "Employee": clean_employee,
        "Artist": clean_artist,
        "Album": clean_album,
        "Invoice": clean_invoice,
        "InvoiceLine": clean_invoiceline,
        "Genre": clean_genre,
        "MediaType": clean_mediatype,
        "Playlist": clean_playlist,
        "PlaylistTrack": clean_playlisttrack,
        "Track": clean_track,
    }
    func = switcher.get(table_name, lambda df: df)
    return func(df)

def create_cleaned_table_like_raw(session, table_name):
    create_sql = f'''
        CREATE TABLE IF NOT EXISTS "{cleaned_schema}"."{table_name.upper()}" LIKE "{raw_schema}"."{table_name.upper()}"
    '''
    print(f"Creating cleaned table {cleaned_schema}.{table_name.upper()} if not exists...")
    session.sql(create_sql).collect()

def main():
    with Session.builder.configs(snowflake_params).create() as session:
        create_cleaning_stage(session)
        create_cleaned_schema(session)

        tables = ["Customer", "Employee", "Artist", "Album", "Invoice", "InvoiceLine",
                  "Genre", "MediaType", "Playlist", "PlaylistTrack", "Track"]

        for table in tables:
            print(f"\nProcessing table: {table}")
            csv_input = f"{table}.csv"
            
            if not os.path.exists(csv_input):
                print(f"ERROR: CSV file {csv_input} not found. Skipping {table}.")
                continue

            print(f"Loading raw data from {csv_input} ...")
            df = pd.read_csv(csv_input)
            print(f"Loaded {len(df)} rows.")

            cleaned_df = clean_data(df, table)
            print(f"Cleaned {len(cleaned_df)} rows.")

            csv_file = export_to_csv(cleaned_df, table)

            upload_to_stage(session, csv_file)

            create_cleaned_table_like_raw(session, table)

            truncate_cleaned_table(session, table)

            copy_into_cleaned_table(session, table, csv_file)

            print(f"{table} cleaned, uploaded, and loaded successfully.")

if __name__ == "__main__":
    main()
