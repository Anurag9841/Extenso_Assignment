from extract import extract_voters
from transform import get_tables_rows,create_file_name
from load import save_csv
from bs4 import BeautifulSoup
from transform import data
'''
the code below iteratively fetches voter data from a website, parses it,
 and saves it into CSV files.
'''

for datas in data.keys():
    for i in data[datas]:
        voters = extract_voters(state="3", district="26",vdc_mun="5277", ward=datas, reg_centre=i)
        soup = BeautifulSoup(voters, "html.parser")
        tables = soup.find_all("table")
        column_names = ['sn', 'voter_num', 'voter_name',
                            'age', 'gender', 'spouse_name', 'parents_name']
        voters_record = get_tables_rows(tables[5])
        file_name = create_file_name(tables[3])
        save_csv(file_name, column_names, voters_record)


