import pandas as pd
from db_utils import PostgresDB

import os
import pandas as pd

# Set the directory containing the CSV files
folder_path = "./output"
# Check if the folder exists
if not os.path.exists(folder_path):
    raise FileNotFoundError(
        f"Folder not found: {folder_path} (working dir: {os.getcwd()})"
    )

# List all CSV files in the folder
csv_files = [f for f in os.listdir(folder_path) if f.endswith(".csv")]

for file in csv_files:

    df = pd.read_csv(os.path.join(folder_path, file))
    df.Name = os.path.splitext(file)[0]

    print(f"DataFrame Name: {df.Name}")
    print(f"DataFrame Shape: {df.shape}")

    db = PostgresDB()

    # Load the DataFrame to PostgreSQL
    db.load_to_postgres(df, table_name=df.Name)

    # Load the DataFrame to Neon PostgreSQL
    db.load_to_neon_postgres(df, table_name=df.Name)
