from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *
import pymysql

spark = SparkSession.builder.appName("field_mapping_modified").config("spark.jars","C:\spark-3.5.1-bin-hadoop3\jars\mysql-connector-j-8.4.0.jar").getOrCreate()

def connection_properties():
    """
    Establishes a connection to the MySQL database and returns the connection,
    properties dictionary for Spark JDBC, and the JDBC URL.
    """
    connection = pymysql.connect(
            host='localhost',
            user='root',
            password='root',
            database='main_database')

    url = "jdbc:mysql://localhost:3306/main_database"
    properties = {
        "user": "root",
        "password": "root",
        "driver": "com.mysql.jdbc.Driver"
    }
    return connection,properties,url

connection,properties,url = connection_properties()

def sql_table_updater(index,interval_by):
    """
    Updates the execution date, start date, and end date in the 'cf_etl_table' table.
    Commits the changes to the database.
    """
    try:
        with connection.cursor() as cursor:
            try:
                exec_date = f'update `main_database`.cf_etl_table set execution_date = current_timestamp where id = {index + 1}'
                cursor.execute(exec_date)
            except Exception as e:
                print(f"Error updating execution date: {e}")
                return

            try:
                start_date = f'update `main_database`.cf_etl_table set start_date_time = date_add(start_date_time, interval {interval_by} day)'
                cursor.execute(start_date)
            except Exception as e:
                print(f"Error updating start date: {e}")
                return

            try:
                end_date = f'update `main_database`.cf_etl_table set end_date_time = date_add(end_date_time, interval {interval_by} day)'
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


def mapping(url,table_name,properties):
    df = spark.read.jdbc(url=url, table=table_name, properties=properties)
    rows = df.collect()
    for row in rows:
        interval_by,id,location,hdfs_file_name,inc_field,database_name,table_name,start_date,end_date,partition_by,is_inc =row['interval_days'],row['id'],row['location'],row['hdfs_file_name'],row['inc_field'],row['Schema_names'],row['Table_names'],row['start_date_time'],row['end_date_time'],row['partition_by'],row['is_incremental']
        hdfs_path = f'{location}{hdfs_file_name}'
        cursor = connection.cursor()
        cursor.callproc('main_database.executor_mappings_without_cursor', [id])
        result = cursor.fetchone()[0]
        jdbc_url = f"jdbc:mysql://localhost:3306/{database_name}"
        if is_inc:
            query = f"(SELECT {result} FROM {database_name}.{table_name} WHERE {inc_field} BETWEEN '{start_date}' AND '{end_date}') as selected_data"
            hadoop_conf = spark._jsc.hadoopConfiguration()
            fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
            path = spark._jvm.org.apache.hadoop.fs.Path(hdfs_path)
            if fs.exists(path):
                existing_data = spark.read.parquet(hdfs_path)
                dataframe = spark.read.jdbc(url=jdbc_url, table=query, properties=properties)
                merged_data = dataframe.unionByName(existing_data).dropDuplicates([partition_by])
                merged_data.write.mode('overwrite').parquet(hdfs_path,partitionBy=[partition_by])
            else:
                dataframe.write.mode('overwrite').parquet(hdfs_path,partitionBy=[partition_by])
            sql_table_updater(rows.index(row), interval_by)
        else:
            query = f"(SELECT {result} FROM {database_name}.{table_name}) as selected_data"
            dataframe = spark.read.jdbc(url=jdbc_url, table=query, properties=properties)
            dataframe.write.mode('overwrite').parquet(hdfs_path) 

mapping(url,'cf_etl_table',properties)

def data_show(hdfs_file_name):
    """
    Reads and displays data from a Parquet file in HDFS.
    """
    data = spark.read.parquet(f'hdfs://localhost:19000/airflow/{hdfs_file_name}')
    return data.show()

print(data_show('sample_data'))

