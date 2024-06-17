from aggregate_function import table,config,get_start_last_transaction_date,new_data,Schema,df
from get_data import rw_transaction_data,product_category_map

def main():
    final_table = new_data(df,rw_transaction_data,product_category_map)
    return final_table