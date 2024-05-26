import pandas as pd
def merged():
    reward_point = pd.read_csv("/opt/airflow/dags/input/reward_point.csv")
    monthly_amount = pd.read_csv("/opt/airflow/dags/input/monthly_amount.csv")
    total_amount = pd.read_csv("/opt/airflow/dags/input/total_amount.csv")
    Count_pivot_table_total_amt = pd.read_csv("/opt/airflow/dags/input/Count_pivot_table_total_amt.csv")
    monthly_count = pd.read_csv("/opt/airflow/dags/input/monthly_count.csv")
    latest_transaction = pd.read_csv("/opt/airflow/dags/input/latest_transaction.csv")
    latest_product = pd.read_csv("/opt/airflow/dags/input/latest_product.csv")
    total_revenue = pd.read_csv("/opt/airflow/dags/input/total_revenue.csv")
    most_used_product = pd.read_csv("/opt/airflow/dags/input/most_used_product.csv")
    second_most_used_product = pd.read_csv("/opt/airflow/dags/input/second_most_used_product.csv")
    third_most_used_product = pd.read_csv("/opt/airflow/dags/input/third_most_used_product.csv")
    product_usage = pd.read_csv("/opt/airflow/dags/input/product_usage.csv")
    dataframes = [
        reward_point,
        monthly_amount,
        total_amount,
        Count_pivot_table_total_amt,
        monthly_count,
        latest_transaction,
        latest_product,
        total_revenue,
        most_used_product,
        second_most_used_product,
        third_most_used_product,
        product_usage
    ]

    merged_df = dataframes[0]
    for df in dataframes[1:]:
        merged_df = pd.merge(merged_df, df, on='payer_account_id', how='left')
    merged_df.to_csv("/opt/airflow/dags/final_output/product_usage.csv",index = False)