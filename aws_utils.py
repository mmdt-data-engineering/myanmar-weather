import os
import boto3
from dotenv import load_dotenv
from io import StringIO
import pandas as pd
from Logger import Logger
from datetime import date


class AmazonS3:
    def __init__(self):
        # Load environment variables from .env file
        load_dotenv()

        self.aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
        self.aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        self.aws_region = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
        self.bucket_name = os.getenv("AWS_S3_BUCKET_NAME", "mmdtdeprojectbucket")
        self.logger = Logger().get_logger("AmazonS3")
        self.logger.info("AWS initialized")

    def upload_file(self, file_path):

        if self.aws_access_key is None or self.aws_secret_key is None:
            raise ValueError(
                "AWS credentials not found. Please set the AWS_S3_ACCESS_KEY and AWS_S3_SECRET_KEY environment variables."
            )

        # Initialize the S3 client
        s3_client = boto3.client(
            "s3",
            aws_access_key_id=self.aws_access_key,
            aws_secret_access_key=self.aws_secret_key,
            region_name=self.aws_region,
        )

        today = date.today().strftime("%Y-%m-%d")  # Output like '2025-05-16'

        df = pd.read_csv(file_path)

        # Convert DataFrame to CSV in-memory
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)

        try:
            s3_client.put_object(
                Bucket=self.bucket_name,
                Key=today + "_" + str.os.path.basename(file_path),
                Body=csv_buffer.getvalue(),
            )

            info = f"File {file_path} uploaded to S3 bucket {self.bucket_name} successfully."
            self.logger.info(info)
            print(info)

        except Exception as e:
            self.logger.error(f"Error uploading file: {e}")
            raise

    def download_file(self, object_key, destination_file):
        s3_client = boto3.client(
            "s3",
            aws_access_key_id=self.aws_access_key,
            aws_secret_access_key=self.aws_secret_key,
            region_name=self.aws_region,
        )

        try:
            s3_client.download_file(self.bucket_name, object_key, destination_file)

            info = f"File downloaded successfully: {destination_file}"
            self.logger.info(info)
            print(info)
        except Exception as e:
            print(f"Error downloading file: {e}")
            self.logger.error(f"Error downloading file: {e}")
