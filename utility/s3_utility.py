import boto3


class S3Helper:
    def __init__(self, bucket: str, profile=None):
        """
        bucket: Bucket name
        profile: Which set of aws account and secret you use, if you use default, you don't need to input anything.
        """
        if profile:
            self.s3 = boto3.Session(profile_name=profile).client("s3")
        else:
            self.s3 = boto3.client("s3")
        self.bucket = bucket

    def upload_file_stream(self, content: str, object_name: str) -> bool:
        try:
            self.s3.put_object(Body=content, Bucket=self.bucket, Key=object_name)
        except Exception as ex:
            print("upload file failed")
            print(ex)
            return False
        return True

    def upload_file(self, file_path: str, object_name: str):
        try:
            self.s3.upload_file(file_path, self.bucket, object_name)

        except Exception as ex:
            print(f"upload file {file_path} failed")
            print(ex)
            return False
        return True

    def download_file(self, object_name: str, file_path: str):
        try:
            self.s3.download_file(self.bucket, object_name, file_path)
        except Exception as ex:
            print(f"upload file {file_path} failed")
            print(ex)

    def download_file_stream(self, object_name: str) -> str:  # type: ignore
        try:
            response = self.s3.get_object(Bucket=self.bucket, Key=object_name)
            return response["Body"].read().decode("utf-8")
        except Exception as ex:
            print("download file failed")
            print(ex)


vsx_s3 = S3Helper("vsx-warehouse-data")
vsx_s3.upload_file_stream("This is a test", "dev/test/test2.txt")
test_str = vsx_s3.download_file_stream("dev/test/test2.txt")
print(test_str)

# vsx_s3.upload_file(
#     "/Users/yuyv/py_projects/vsx_data_warehouse/data/mixpanel/Login/20231227105536.json",
#     "dev/test/login_test.json",
# )
