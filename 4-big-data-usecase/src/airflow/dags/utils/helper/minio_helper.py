from minio import Minio
from minio.error import S3Error

class MinioHelper:
    def __init__(self, endpoint, access_key, secret_key, secure=False):
        self.minio_client = Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=secure)
        
    def ensure_bucket_exists(self, bucket_name):
        if not self.minio_client.bucket_exists(bucket_name):
            self.minio_client.make_bucket(bucket_name)
            return f"Created bucket: {bucket_name}"
        else:
            return f"Bucket {bucket_name} already exists."
        
    def upload_file(self, bucket_name, object_name, file_path, content_type="application/octet-stream"):
        try:
            self.ensure_bucket_exists(bucket_name)
            self.minio_client.fput_object(bucket_name, object_name, file_path, content_type=content_type)
            return f"File {file_path} uploaded to {bucket_name}/{object_name}"
        except S3Error as err:
            return err