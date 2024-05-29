import mysql.connector
import pandas as pd

def preporcess():
    mydb = mysql.connector.connect(
      host = "host.docker.internal",
      user = "root",
      password = "root",
      database = "extenso_assignment"
    )
    cursor = mydb.cursor()
    query = "show tables"
    cursor.execute(query)
    tables = cursor.fetchall()
    def database(table_name: str) -> pd.DataFrame:
        query = f"select * from {table_name}"
        cursor.execute(query)
        result = cursor.fetchall()
        columns = [col[0] for col in cursor.description]
        return pd.DataFrame(result, columns=columns)
    customer_profile = database('customer_profile')
    product_category = database("product_category")
    product_category_map = database("product_category_map")
    products = database("products")
    rw_transaction_data = database("rw_transaction_data")
    rw_transaction_data["last_modified_date"] = pd.to_datetime(rw_transaction_data["last_modified_date"])
    rw_transaction_data['month'] = pd.to_datetime(rw_transaction_data['last_modified_date']).dt.month
    rw_transaction_data['week'] = pd.to_datetime(rw_transaction_data['last_modified_date']).dt.isocalendar().week
    joined_Df = pd.merge(rw_transaction_data, product_category_map, on=['product_id', 'product_type_id','module_id'], how='inner')
    total_pivot_table = pd.pivot_table(joined_Df, values='amount', index=['payer_account_id','month'], columns='txn_flow', aggfunc='sum')
    total_pivot_table = total_pivot_table.reset_index()
    monthly_amount = total_pivot_table.groupby(["payer_account_id"]).mean().reset_index().rename(columns={"InFlow":"Monthly_Inflow_Amount","OutFlow":"Monthly_OutFlow_Amount","Value Chain":"Monthly_Value_Chain"}).drop(columns = "month",axis=1)
    total_amount = total_pivot_table.groupby("payer_account_id").sum().reset_index().rename(columns={"InFlow": "Total_inflow_amount","OutFlow":"Total_outflow_amount","Value Chain":"Total_value_chain_amount"})\
    .drop(columns="month",axis=1)
    count_pivot_table  = pd.pivot_table(joined_Df, values='amount', index=['payer_account_id','month'], columns='txn_flow', aggfunc='count')
    count_pivot_table = count_pivot_table.reset_index()
    Count_pivot_table_total_amt = count_pivot_table.groupby("payer_account_id").sum().reset_index()\
    .rename(columns={"InFlow":"total_Inflow_Count","OutFlow":"total_Outflow_Count","Value Chain": "total_Value_Chain_count"}).drop(columns="month",axis=1)
    monthly_count = count_pivot_table.groupby(["payer_account_id"]).mean().reset_index().rename(columns={"InFlow":"Monthly_Inflow_Count","OutFlow":"Monthly_Outflow_Count","Value Chain": "Monthly_Value_Chain_count"})\
    .drop(columns="month",axis=1)
    reward_point = joined_Df.groupby("payer_account_id")["reward_point"].sum().reset_index()
    latest_transaction = joined_Df.groupby("payer_account_id")["last_modified_date"].max().reset_index()\
    .rename(columns={"last_modified_date":"latest_transaction_date"})
    latest_product = joined_Df.sort_values(['payer_account_id', 'last_modified_date'],ascending = [True,False])
    result = latest_product.drop_duplicates('payer_account_id')
    latest_product = result[['payer_account_id', 'last_modified_date','product_name']].rename(columns={"last_modified_date":"latest_transaction_date","product_name":"latest_used_product"})
    revenue_amount = joined_Df.groupby(['payer_account_id','month']).agg({'revenue_amount':'sum'})
    total_revenue = revenue_amount.groupby(['payer_account_id']).agg(total_revenue=('revenue_amount', 'sum'),monthly_revenue=('revenue_amount', 'mean')).reset_index()
    datas_count = joined_Df.groupby(["payer_account_id", "product_name"])["product_name"].count().reset_index(name="count")
    datas_count_sorted = datas_count.sort_values(by=["payer_account_id", "count"], ascending=[True, False])
    most_used_product = datas_count_sorted.groupby("payer_account_id").nth(0).reset_index().rename(columns={"product_name":"most_used_product"})[['payer_account_id', 'most_used_product']]
    second_most_used_product = datas_count_sorted.groupby("payer_account_id").nth(1).reset_index().rename(columns={"product_name":"second_most_used_product"})\
    [['payer_account_id', 'second_most_used_product']]
    third_most_used_product = datas_count_sorted.groupby("payer_account_id").nth(2).reset_index().rename(columns={"product_name":"third_most_used_product"})\
    [['payer_account_id', 'third_most_used_product']]
    product_usage = datas_count.groupby("payer_account_id")['product_name'].count().reset_index().rename(columns={"product_name":"product_usage"})

    reward_point.to_csv("/opt/airflow/dags/input/reward_point.csv",index = False)
    monthly_amount.to_csv("/opt/airflow/dags/input/monthly_amount.csv",index = False)
    total_amount.to_csv("/opt/airflow/dags/input/total_amount.csv",index = False)
    Count_pivot_table_total_amt.to_csv("/opt/airflow/dags/input/Count_pivot_table_total_amt.csv",index = False)
    monthly_count.to_csv("/opt/airflow/dags/input/monthly_count.csv",index = False)
    latest_transaction.to_csv("/opt/airflow/dags/input/latest_transaction.csv",index = False)
    latest_product.to_csv("/opt/airflow/dags/input/latest_product.csv",index = False)
    total_revenue.to_csv("/opt/airflow/dags/input/total_revenue.csv",index = False)
    most_used_product.to_csv("/opt/airflow/dags/input/most_used_product.csv",index = False)
    second_most_used_product.to_csv("/opt/airflow/dags/input/second_most_used_product.csv",index = False)
    third_most_used_product.to_csv("/opt/airflow/dags/input/third_most_used_product.csv",index = False)
    product_usage.to_csv("/opt/airflow/dags/input/product_usage.csv",index = False)

