from hdfs import InsecureClient
client = InsecureClient('http://localhost:9870')
local_csv_file_path = "F:\IRIS.csv"
hdfs_upload_path = '/anurag'
client.upload(hdfs_upload_path, local_csv_file_path)
print("file uploaded sucessfully to hdfs")