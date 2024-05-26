'''
This is used to translate the CSV from nepali to english
'''

import pandas as pd
from csv_trans import translate
print(translate(r'C:\Users\acer\PycharmProjects\airflol\scrap-election-commission-nepal-main\combined_file.csv', 'ne','en',sep=','))