# -*- coding: utf-8 -*-

# import pandas as pd
from selenium import webdriver
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.options import Options
from datetime import datetime, date, timedelta
import re
import credentials
import psycopg2
from subprocess import call

### variaveis
DATABASE, HOST, USER, PASSWORD = credentials.setDatabaseLogin()
WAIT = 60
table_icu = 'covid_19.local_hospitalization'
table_beds = 'covid_19.local_beds'
current_date = date.today()-timedelta(days=1)
driver_path = '/home/ubuntu/scripts/load-dados-reclame-aqui/chromedriver'
url = "https://www.covid-19.pa.gov.br/"
url_beds = "https://www.covid-19.pa.gov.br/public/dashboard/2e4b12cd-4e12-4aa2-9d7d-1e3cae29a65f#theme=night"
state_name = 'Pará'

def _Chrome(driver_path):
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("log-level=OFF")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("start-maximized")
    driver = webdriver.Chrome(executable_path=driver_path, options=chrome_options)

    return driver

def _Postgres(DATABASE, USER, HOST, PASSWORD):
    ### conecta no banco de dados
    db_conn = psycopg2.connect("dbname='{}' user='{}' host='{}' password='{}'".format(DATABASE, USER, HOST, PASSWORD))
    cursor = db_conn.cursor()
    print('Connected to the database')

    return (db_conn,cursor)

def getDate(driver):
    element_present = EC.visibility_of_element_located((By.XPATH, "//span[contains(text(),'Atualizado')]"))
    str_date = WebDriverWait(driver, WAIT).until(element_present).text
    match = re.search(r'\d{2}\/\d{2}\/\d{4}',str_date).group()
    last_date = datetime.strptime(match, '%d/%m/%Y').date()

    return last_date

def parseHTMLTable(driver):
    # from selenium.webdriver.common.keys import Keys
    # from selenium.common.exceptions import TimeoutException

    # html = driver.find_element_by_tag_name('html')
    # element_present = EC.presence_of_element_located((By.CLASS_NAME, 'q-table'))
    # wait = WebDriverWait(driver, 6)
    # count = 0

    # while count < 10:
        # html.send_keys(Keys.PAGE_DOWN)
        # try:
            # channel_text = wait.until(element_present)
            # break
        # except TimeoutException:
            # pass
        # count += 1
    # table = driver.find_element_by_xpath('//div[contains(text(),"Leitos exclusivos para COVID-19")]/ancestor::node()[3]//div[@class="q-table__middle scroll"]').get_attribute('innerHTML')
    # df_list = pd.read_html(table)

    # return df_list[0]
    
    element_present = EC.visibility_of_element_located((By.XPATH, "(//h1)[3]"))
    WebDriverWait(driver, WAIT).until(element_present)
    boxes = [item.text for item in driver.find_elements_by_xpath('//h1')]
    nursery_rate = float(boxes[0].strip('%'))
    icu_rate = float(boxes[1].strip('%'))
    nursery_beds = int(boxes[2].replace(',',''))
    icu_beds = int(boxes[3].replace(',',''))
    
    icu = round(icu_beds*icu_rate/100)
    inpatients = icu + round(nursery_beds*nursery_rate/100)

    return (icu,inpatients,icu_beds,nursery_beds)

def parseDF(df):
    nursery_beds = df[df['Tipo de Leito'] == 'Clínico'].iloc[0]['Total']
    icu_beds = df[df['Tipo de Leito'] == 'UTI Adulto'].iloc[0]['Total']
    icu = icu_beds - df[df['Tipo de Leito'] == 'UTI Adulto'].iloc[0]['Disponível']
    inpatients = nursery_beds - df[df['Tipo de Leito'] == 'Clínico'].iloc[0]['Disponível'] + icu

    return (icu,inpatients,icu_beds,nursery_beds)

def insertDB(db_conn,cursor,query):
    print(query)
    cursor.execute(query)
    db_conn.commit()

def main():
    driver = _Chrome(driver_path)
    driver.get(url)
    last_date = getDate(driver)

    if last_date == current_date:

        driver.get(url_beds)
        icu,inpatients,icu_beds,nursery_beds = parseHTMLTable(driver)
        driver.quit()

        #icu,inpatients,icu_beds,nursery_beds = parseDF(df)

        db_conn,cursor = _Postgres(DATABASE, USER, HOST, PASSWORD)
        query = f"INSERT INTO {table_icu} VALUES ('{str(current_date)}','{state_name}',{inpatients},{icu})"
        insertDB(db_conn,cursor,query)

        query = f"INSERT INTO {table_beds} VALUES ('{str(current_date)}','{state_name}','ICU',{icu_beds})"
        insertDB(db_conn,cursor,query)
        query = f"INSERT INTO {table_beds} VALUES ('{str(current_date)}','{state_name}','Nursery',{nursery_beds})"
        insertDB(db_conn,cursor,query)

        cursor.close()
        db_conn.close()

        ### VACUUM ANALYZE
        call('psql -d torkcapital -c "VACUUM ANALYZE '+table_icu+'";',shell=True)
        call('psql -d torkcapital -c "VACUUM ANALYZE '+table_beds+'";',shell=True)

    else:
        raise ValueError('No data for ' + str(current_date))
        driver.quit()

if __name__=="__main__":
    main()