import sqlite3
import pandas as pd


# Specify the path to your SQLite database and the table name
db_path = 'health_events_data.db'
table_name = 'events'

# Load data into DataFrame

def standardize_text(df):
    for column in df.select_dtypes(include=['object']).columns:
        # Convert to uppercase and strip leading/trailing spaces
        df[column] = df[column].str.upper().str.strip()
    return df




def clean_data(db_path, table_name):
    # Establish a connection to the SQLite database
    conn = sqlite3.connect(db_path)
    
    # Define the query to select all data from the specified table
    query = f"SELECT * FROM {table_name}"
    
    # Load data into a Pandas DataFrame
    df = pd.read_sql_query(query, conn)
    conn.close()
    df.replace('', pd.NA, inplace=True)
    for col in df:


        if col == "Cost of Damages ($)" or col == "Affected Population":
            print(col)
            df[col] = pd.to_numeric(df[col], errors='coerce')
            mean_value = df[col].mean()
            print(mean_value)
            df[col].fillna(mean_value, inplace=True)
        else:
            mode_value = df[col].mode()[0]  # mode() returns a Series, take the first value
            print(mode_value)
            df[col].fillna(mode_value, inplace=True)
    # Fill missing categorical values with the mode of the column
    df = df.drop_duplicates()
    df = standardize_text(df)
   
    
    conn = sqlite3.connect('cleaned_data.db')
    df.to_sql('cleaned_data', conn, if_exists='replace', index=False)
    print("DataFrame saved to cleaned_data.db") 
    # Close the connection
    
    


clean_data(db_path, table_name)
