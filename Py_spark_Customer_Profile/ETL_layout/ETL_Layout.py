from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *
import pymysql


# Establish a connection to the MySQL database
connection = pymysql.connect(
        host='localhost',
        user='root',
        password='root',
        database='main_database'
    )

# Initialize a Spark session with the necessary configurations
spark = SparkSession.builder.appName("hadoop_add_file").config("spark.jars", "C:\spark-3.5.1-bin-hadoop3\jars\mysql-connector-j-8.4.0.jar").getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
# JDBC URL and connection properties for MySQL
url = "jdbc:mysql://localhost:3306/main_database"
properties = {
    "user": "root",
    "password": "root",
    "driver": "com.mysql.jdbc.Driver"
}
# Function to update the SQL table with error handling
def sql_table_updater(index):
    try:
        with connection.cursor() as cursor:
            try:
                exec_date = f'update `main_database`.cf_etl_table set execution_date = current_timestamp where id = {index + 1}'
                cursor.execute(exec_date)
            except Exception as e:
                print(f"Error updating execution date: {e}")
                return

            try:
                start_date = f'update `main_database`.cf_etl_table set start_date_time = date_add(start_date_time, interval 1 day)'
                cursor.execute(start_date)
            except Exception as e:
                print(f"Error updating start date: {e}")
                return

            try:
                end_date = f'update `main_database`.cf_etl_table set end_date_time = date_add(end_date_time, interval 1 day)'
                cursor.execute(end_date)
            except Exception as e:
                print(f"Error updating end date: {e}")
                return

            try:
                connection.commit()
            except Exception as e:
                print(f"Error committing transaction: {e}")
                connection.rollback()
    except Exception as e:
        print(f"Error in SQL updater: {e}")

# ETL function to read data from MySQL, process it, and write to HDFS
def etl(url, table_name, properties):
    try:
        # Read the entire table from the MySQL database
        df = spark.read.jdbc(url=url, table=table_name, properties=properties)
    except Exception as e:
        print(f"Error reading from JDBC: {e}")
        return
    # Collect all rows from the DataFrame
    rows = df.collect()

    # Iterate over each row in the DataFrame
    for row in rows:
        is_inc = row['is_incremental']
        location,hdfs_file_name,inc_field,database_name,table_name = row['location'],row['hdfs_file_name'],row['inc_field'],row['Schema_names'],row['Table_names']
        jdbc_url = f"jdbc:mysql://localhost:3306/{database_name}"
        hdfs_path = f'{location}{hdfs_file_name}'
        if is_inc:
            # If the row is incremental, construct the query and read the specific data
            start_date,end_date= row['start_date_time'],row['end_date_time']
            query = f"(SELECT * FROM {database_name}.{table_name} WHERE {inc_field} BETWEEN '{start_date}' AND '{end_date}') as selected_data"
            dataframe = spark.read.jdbc(url=jdbc_url, table=query, properties=properties)
            try:
                dataframe.write.mode('append').parquet(hdfs_path)
            except Exception as e:
                print(f"Error appending to HDFS: {e}")
            sql_table_updater(rows.index(row))
        else:
            # If the row is not incremental, read the entire table
            dataframe = spark.read.jdbc(url=jdbc_url, table=table_name, properties=properties)
            try:
                dataframe.write.mode("overwrite").parquet(hdfs_path)
            except Exception as e:
                print(f"Error overwriting to HDFS: {e}")

etl(url,'cf_etl_table',properties)

# Function to read and show data from a Parquet file in HDFS
def data_show():
    data = spark.read.parquet('hdfs://localhost:19000/anurag/sample_data')
    return data.show()

print(data_show())

