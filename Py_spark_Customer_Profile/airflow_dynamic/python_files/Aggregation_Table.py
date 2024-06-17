from aggregate_function import agg_table,schema,spark

def main1():
    final_table = spark.read.csv('/opt/airflow/input_file/final_table', header=True, schema=schema)
    agg_table(final_table)
