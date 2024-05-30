import pandas as pd
import numpy as np
import os

def sanitize(df):
    return df.applymap(
        lambda x: x.replace(',', '').replace(' ', '').strip() if isinstance(x, str) else '' if pd.isna(x) else x)


def load_df_from_dir(dir_path='/opt/airflow/files/input_files'):
    csv_files = [f for f in os.listdir(dir_path) if f.endswith(".csv")]
    layouts = []

    for csv_file in csv_files:
        file_path = os.path.join(dir_path, csv_file)
        df = pd.read_csv(file_path)
        df['source'] = csv_file
        layouts.append(df)
    return layouts




