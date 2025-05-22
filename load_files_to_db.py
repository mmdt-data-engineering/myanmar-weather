from load_to_db import get_csv_files, load_file_to_db
import os
from datetime import date


def load_all_csv_files_to_db():
    """Load CSV files from a folder to Neon PostgreSQL database."""

    folder_path = "./output/"
    csv_files = get_csv_files(folder_path)

    for file in csv_files:
        print(f"Loading file: {file}")
        load_file_to_db(os.path.join(folder_path, file))


load_all_csv_files_to_db()
