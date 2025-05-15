import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os


class PostgresDB:
    def __init__(self):
        pass

    def load_to_postgres(self, df: pd.DataFrame, table_name: str) -> None:
        """
        Load the DataFrame to a PostgreSQL database.

        Parameters:
        df (DataFrame): DataFrame to be loaded
        table_name (str): Table Name

        Returns:
        None

        """
        # Load environment variables
        load_dotenv()
        DB_URL = os.getenv("POSTGRES_URL")
        print(f"POSTGRES_URL: {DB_URL}")

        # Create a connection to the PostgreSQL database
        engine = create_engine(DB_URL)
        conn = engine.connect()

        # Save the DataFrame to the database
        df.to_sql(table_name, conn, if_exists="replace", index=False)

        # Close the connection
        conn.close()

    def load_to_neon_postgres(self, df: pd.DataFrame, table_name: str) -> None:
        """
        Save the DataFrame to a Neon Server PostgreSQL

        Parameters:
        df (DataFrame): DataFrame to be loaded
        table_name (str): Table Name

        Returns:
        None

        """
        # Load environment variables
        load_dotenv()
        DB_URL = os.getenv("NEON_POSTGRES_URL")
        print(f"POSTGRES_URL: {DB_URL}")

        # Create a connection to the PostgreSQL database
        engine = create_engine(DB_URL)
        conn = engine.connect()

        # Save the DataFrame to the database
        df.to_sql(table_name, conn, if_exists="replace", index=False)

        # Close the connection
        conn.close()
