# -*- coding: utf-8 -*-

from pyvirtualdisplay import Display
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
url = "https://superset.plataformatarget.com.br/superset/dashboard/72/"
state_name = 'Paraíba'
indir = '/home/ubuntu/dump/dados_covid_19/beds/'
display = Display(visible=0, size=(1536,864))

def _Chrome(driver_path,indir):
    chrome_options = Options()
    chrome_options.add_argument("log-level=OFF")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("start-maximized")
    prefs = {"download.default_directory" : indir}
    chrome_options.add_experimental_option("prefs",prefs)
    driver = webdriver.Chrome(executable_path=driver_path, options=chrome_options)

    return driver

def _Postgres(DATABASE, USER, HOST, PASSWORD):
    ### conecta no banco de dados
    db_conn = psycopg2.connect("dbname='{}' user='{}' host='{}' password='{}'".format(DATABASE, USER, HOST, PASSWORD))
    cursor = db_conn.cursor()
    print('Connected to the database')

    return (db_conn,cursor)

def getDate(driver):
    element_present = EC.visibility_of_element_located((By.XPATH, '//em[contains(text(),"Atualizado")]'))
    str_date = WebDriverWait(driver, WAIT).until(element_present).text
    match = re.search(r'\d{2}\/\d{2}\/\d{4}',str_date).group()
    last_date = datetime.strptime(match, '%d/%m/%Y').date()

    return last_date

def downloadCSV(driver,WAIT,bed,indir):
    button = EC.element_to_be_clickable((By.XPATH, f'//span/input[@value="{bed} COVID-19 PB"]/parent::span/parent::div//button'))
    WebDriverWait(driver, WAIT).until(button).click()
    csv = EC.element_to_be_clickable((By.XPATH, f'//input[@value="{bed} COVID-19 PB"]/parent::span/parent::div//a[text() = "Exportar CSV"]'))
    WebDriverWait(driver, WAIT).until(csv).click()
    inpatients,bed_number = parseCSV(indir)

    return (inpatients,bed_number)

def parseCSV(indir):
    import csv
    from time import sleep
    import os

    sleep(1)
    while not os.listdir(indir):
        print('Downloading file...')
        sleep(3)
    _file = [f for f in os.listdir(indir) if f.endswith('.csv')][0]

    with open(indir+_file, 'r', encoding="utf-8") as ifile:
        reader = csv.reader(ifile, delimiter=';')
        header = next(reader, None)  ### Pula o cabeçalho
        for row in reader:
            if row[0] == 'Disponível':
                available = int(float(row[1]))
            elif row[0] == 'Ocupado':
                inpatients = int(float(row[1]))
    bed_number = available + inpatients
    os.remove(indir+_file)

    return (inpatients,bed_number)

def insertDB(db_conn,cursor,query):
    print(query)
    cursor.execute(query)
    db_conn.commit()

def main():
    display.start()
    driver = _Chrome(driver_path,indir)
    driver.get(url)
    last_date = getDate(driver)

    if last_date == current_date:

        icu,icu_beds = downloadCSV(driver,WAIT,"UTIs",indir)
        nursery,nursery_beds = downloadCSV(driver,WAIT,"Enfermarias",indir)
        driver.quit()
        display.stop()

        inpatients = nursery + icu

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
        display.stop()

if __name__=="__main__":
    main()