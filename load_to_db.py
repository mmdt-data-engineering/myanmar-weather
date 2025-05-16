import pandas as pd
from db_utils import PostgresDB

import os
import pandas as pd


def get_csv_files(folder_path: str) -> list:
    """
    Get a list of CSV files in the specified folder.
    """
    # Check if the folder exists
    if not os.path.exists(folder_path):
        raise FileNotFoundError(
            f"Folder not found: {folder_path} (working dir: {os.getcwd()})"
        )

    # List all CSV files in the folder
    csv_files = [f for f in os.listdir(folder_path) if f.endswith(".csv")]
    return csv_files


def load_to_local_postgres():
    """Load CSV files from a folder to local PostgreSQL database."""

    folder_path = "./output"
    csv_files = get_csv_files(folder_path)

    for file in csv_files:

        df = pd.read_csv(os.path.join(folder_path, file))
        df.Name = os.path.splitext(file)[0]

        print(f"DataFrame Name: {df.Name}")
        print(f"DataFrame Shape: {df.shape}")

        db = PostgresDB()

        # Load the DataFrame to local PostgreSQL
        db.load_to_postgres(df, table_name=df.Name)


def load_to_neon_postgres():
    """Load CSV files from a folder to Neon PostgreSQL database."""

    folder_path = "./output"
    csv_files = get_csv_files(folder_path)

    for file in csv_files:

        df = pd.read_csv(os.path.join(folder_path, file))
        df.Name = os.path.splitext(file)[0]

        print(f"DataFrame Name: {df.Name}")
        print(f"DataFrame Shape: {df.shape}")

        db = PostgresDB()

        # Load the DataFrame to Neon PostgreSQL
        db.load_to_neon_postgres(df, table_name=df.Name)


# load_to_local_postgres()
load_to_neon_postgres()
