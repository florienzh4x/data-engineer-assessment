import json
from datetime import datetime, timedelta
import os

from utils.helper.minio_helper import MinioHelper

class IngestionUtils:
    def __init__(self, config):
        
        self.config = config
        
        self.minio_helper = MinioHelper(
            endpoint=self.config["minio_host"],
            access_key=self.config["minio_credentials"]["access_key"],
            secret_key=self.config["minio_credentials"]["secret_key"]    
        )
        
    def ingest_data(self):
        
        destination_file_name = os.path.join(
            self.config["destination_prefix"],
            self.config["table_name"]+"_"+datetime.now().strftime("%Y%m%d_%H%M%S")+"."+self.config['destination_format']
        )
        
        self.minio_helper.upload_file(
            bucket_name=self.config["destination_bucket"],
            object_name=destination_file_name,
            file_path=self.config["source_file_path"],
            content_type=self.config["destination_content_type"]
        )