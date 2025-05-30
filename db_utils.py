import pandas as pd
from psycopg2 import OperationalError
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
from Logger import Logger


class PostgresDB:
    def __init__(self):
        self.logger = Logger().get_logger("PostgresDB")
        self.logger.info("WeatherAPI initialized")

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

        # Create a connection to the PostgreSQL database
        engine = create_engine(DB_URL)
        # conn = engine.connect()
        try:
            with engine.connect() as conn:

                # Save the DataFrame to the database
                df.to_sql(table_name, conn, if_exists="replace", index=False)

                info = f"{table_name} table is created at the database: {DB_URL}"
                self.logger.info(info)

        except OperationalError as e:
            self.logger.info(f"Connection failed: {e}")

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
        PGHOST = os.getenv("PGHOST")
        PGDATABASE = os.getenv("PGDATABASE")
        PGUSER = os.getenv("PGUSER")
        PGPASSWORD = os.getenv("PGPASSWORD")
        ENDPOINT = os.getenv("ENDPOINT")
        NEON_POSTGRES_URL = f"postgresql://{PGUSER}:{PGPASSWORD}@{PGHOST}/{PGDATABASE}?sslmode=require&options=endpoint={ENDPOINT}"

        # NEON_POSTGRES_URL = os.getenv("NEON_POSTGRES_URL")

        # Create a connection to the PostgreSQL database
        engine = create_engine(url=NEON_POSTGRES_URL)
        # conn = engine.connect()
        try:
            with engine.connect() as conn:

                # Save the DataFrame to the database
                df.to_sql(table_name, conn, if_exists="append", index=False)

                info = f"{table_name} table is created at the database: {NEON_POSTGRES_URL}"
                self.logger.info(info)

        except OperationalError as e:
            self.logger.error(f"Connection failed: {e}")
