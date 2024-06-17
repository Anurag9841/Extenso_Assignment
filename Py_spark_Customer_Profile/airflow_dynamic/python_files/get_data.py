from aggregate_function import table,config,get_start_last_transaction_date,new_data,Schema

start_date,last_date = get_start_last_transaction_date()
product_category_map = table("product_category_map",start_date,last_date)
rw_transaction_data = table("rw_transaction_data",start_date,last_date)

