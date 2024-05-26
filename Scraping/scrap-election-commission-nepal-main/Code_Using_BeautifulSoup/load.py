import pandas as pd
import os
def save_csv(table_name, headers, rows):
    path = os.getcwd() + "/Data"
    pd.DataFrame(rows, columns=headers).to_csv(
        os.path.join(path, f"{table_name}.csv"))
