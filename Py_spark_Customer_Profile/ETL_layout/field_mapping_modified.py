from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *
import pymysql

'''
-> Database used:
    1. main_database 
        a) Contains "Config_Table" with table name saved as main_database.cf_etl_table.
        b) Contains "field_mapping" table saved as main_database.field_mapping.
        c) Contains "field_maping_query" table saved as main_database.field_maping_query.
            'filed_mapping_query contains sub query for field_mapping'
    2. test_database1
        a) Contains "incremental" table name saved as test_database1.sample_data.
    3. test_database2
        a) Contains "non_incremental" table name saved as test_database2.sample_data1.
'''

spark = SparkSession.builder \
    .appName('Field_mapping_modified') \
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:19000") \
    .getOrCreate()



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
    '''
    :param url: url for jdbc
    :param table_name: name of the config table
    :param properties: properties to connect to sql from pyspark
    :return:
    '''
    config_table = spark.read.jdbc(url=url, table=table_name, properties=properties)
    field_mapping_query = spark.read.jdbc(url=url, table='field_mapping_query', properties=properties)
    rows = config_table.collect()
    field_mapping_query_rows = field_mapping_query.collect()
    for idx,row in zip(range(len(rows)),rows):
        interval_by,id,location,hdfs_file_name,inc_field,database_name,table_name,start_date,end_date,partition_by,is_inc =row['interval_days'],row['id'],row['location'],row['hdfs_file_name'],row['inc_field'],row['Schema_names'],row['Table_names'],row['start_date_time'],row['end_date_time'],row['partition_by'],row['is_incremental']
        hdfs_path = f'{location}{hdfs_file_name}'
        statement = field_mapping_query_rows[idx]['field_mapping_query']
        jdbc_url = f"jdbc:mysql://localhost:3306/{database_name}"
        if is_inc:
            query = f"( {statement} WHERE {inc_field} BETWEEN '{start_date}' AND '{end_date}') as selected_data"
            hadoop_conf = spark._jsc.hadoopConfiguration()
            fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
            path = spark._jvm.org.apache.hadoop.fs.Path(hdfs_path)
            dataframe = spark.read.jdbc(url=jdbc_url, table=query, properties=properties)
            if fs.exists(path):
                existing_data = spark.read.parquet(hdfs_path)
                merged_data = dataframe.unionByName(existing_data).dropDuplicates([partition_by])
                merged_data.write.mode('overwrite').parquet(hdfs_path,partitionBy=[partition_by])
            else:
                dataframe.write.mode('overwrite').parquet(hdfs_path,partitionBy=[partition_by])
            sql_table_updater(rows.index(row), interval_by)
        else:
            query = f"({statement}) as selected_data"
            dataframe = spark.read.jdbc(url=jdbc_url, table=query, properties=properties)
            dataframe.write.mode('overwrite').parquet(hdfs_path)
        return ("sucess")

mapping(url,'cf_etl_table',properties)

def data_show(hdfs_file_name):
    """
    Reads and displays data from a Parquet file in HDFS.
    """
    data = spark.read.parquet(f'hdfs://localhost:19000/airflow/{hdfs_file_name}')
    return data.show()

print(data_show('sample_data'))

