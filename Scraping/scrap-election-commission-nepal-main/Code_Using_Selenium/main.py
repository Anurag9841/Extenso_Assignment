from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.common.exceptions import NoSuchElementException
from bs4 import BeautifulSoup
import pandas as pd
from io import StringIO


webpage_loader = webdriver.Firefox()
webpage_loader.get("https://voterlist.election.gov.np/bbvrs1/index_2.php")
webpage_loader.implicitly_wait(1)

df = pd.DataFrame()


def submit():
    entries_dropdown = webpage_loader.find_element(By.XPATH, '//select[@name="tbl_data_length"]')
    Select(entries_dropdown).select_by_value("100")

    def scraper():
        global df
        page_content = webpage_loader.page_source
        soup = BeautifulSoup(page_content, 'html.parser')
        table = soup.find('table', class_='bbvrs_data dataTable')
        data = pd.read_html(StringIO(str(table)))[0]
        df = pd.concat([df, data], ignore_index=True)

    while True:
        scraper()

        try:
            next_page_link = webpage_loader.find_element(By.CLASS_NAME, "paginate_enabled_next")

            if next_page_link:
                webpage_loader.execute_script("arguments[0].click();", next_page_link)
        except NoSuchElementException:
            print("No pages available")
            break

for state_index in range(3, 4):
    try:
        for dist_index in range(11, 12):
            try:
                for mun_index in range(1, 19):
                    try:
                        for ward_index in range(1, 34):
                            try:
                                for reg_centre_index in range(1, 10):
                                    try:
                                        dropdown = webpage_loader.find_element(By.XPATH, '//select[@id="state"]')
                                        dist_dropdown = webpage_loader.find_element(By.XPATH, '//select[@id="district"]')
                                        mun_dropdown = webpage_loader.find_element(By.XPATH, '//select[@id="vdc_mun"]')
                                        ward_dropdown = webpage_loader.find_element(By.XPATH, '//select[@id="ward"]')
                                        reg_centre_dropdown = webpage_loader.find_element(By.XPATH,
                                                                                  '//select[@id="reg_centre"]')
                                        submit_button = webpage_loader.find_element(By.ID, "btnSubmit")

                                        Select(dropdown).select_by_index(state_index)
                                        Select(dist_dropdown).select_by_index(dist_index)
                                        Select(mun_dropdown).select_by_index(mun_index)
                                        Select(ward_dropdown).select_by_index(ward_index)
                                        Select(reg_centre_dropdown).select_by_index(reg_centre_index)
                                        submit_button.click()
                                        submit()
                                        go_back = webpage_loader.find_element(By.CLASS_NAME, "a_back")
                                        go_back.click()
                                    except NoSuchElementException:
                                        print("Skipping...")
                            except:
                                break
                    except:
                        break
            except NoSuchElementException:
                break
    except NoSuchElementException:
        break

df.to_pickle(r"C:\Users\acer\PycharmProjects\airflol\scrap-election-commission-nepal-main\Data\lalitpur.pkl")

