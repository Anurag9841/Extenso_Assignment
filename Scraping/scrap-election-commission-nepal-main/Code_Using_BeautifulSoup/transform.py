
'''
This file contains two function get_table_rows() and create_file_name()

---> get_tables_rows() is used to get all information about voters from the table
---->create_file_name() is used to save file according to name of voters place
'''
data = {'1':['9631','9625','9627'],
    '2':['11298'],
    '3':['9638'],'4':['9567','11299'],'5':['9570'],'6':['9578'],'7':['9574','12264'],'8':['9583'],'9':['9589']}
def get_tables_rows(table):
    voters_record = []
    for tr in table.find_all("tr")[1:]:
        voter_record = []
        tds = tr.find_all("td")
        for td in tds[0:-1]:
            voter_record.append(td.text.strip())
        voters_record.append(voter_record)
    return voters_record

def create_file_name(table):
    return table.find("tr").find_all("th")[-1].b.text.replace(" ", "-").replace(",", "").replace(".", "")
