# -*- coding: utf-8 -*-

from selenium import webdriver
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.options import Options
from datetime import datetime, date, timedelta
import credentials
import psycopg2
from subprocess import call
import pandas as pd
from os import remove

### variaveis
DATABASE, HOST, USER, PASSWORD = credentials.setDatabaseLogin()
WAIT = 60
tablename = 'covid_19.spain_stg'
current_date = date.today()-timedelta(days=3)
driver_path = '/home/ubuntu/scripts/load-dados-reclame-aqui/chromedriver'
url = "https://covidly.com/graph?country=Spain&state={}"
dict = {
        'Andalusia' : 'AN',
        'Aragon' : 'AR',
        'Asturias' : 'AS',
        'Balearic Islands' : 'IB',
        'Canary Islands' : 'CN',
        'Cantabria' : 'CB',
        'Castile and Leon' : 'CL',
        'Castile La Mancha' : 'CM',
        'Catalonia' : 'CT',
        'Ceuta' : 'CE',
        'Extremadura' : 'EX',
        'Galicia' : 'GA',
        'La Rioja' : 'RI',
        'Madrid' : 'MD',
        'Melilla' : 'ME',
        'Murcia' : 'MC',
        'Navarre' : 'NC',
        'Basque Country' : 'PV',
        'Valencia' : 'VC'
    }
_file = '/home/ubuntu/scripts/load-dados-covid-19/csv/spain.csv'

def _Chrome(driver_path):
    chrome_options = Options()
    chrome_options.add_argument("log-level=OFF")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("start-maximized")
    chrome_options.add_argument("--headless")
    driver = webdriver.Chrome(executable_path=driver_path, options=chrome_options)

    return driver

def _Postgres(DATABASE, USER, HOST, PASSWORD):
    ### conecta no banco de dados
    db_conn = psycopg2.connect("dbname='{}' user='{}' host='{}' password='{}'".format(DATABASE, USER, HOST, PASSWORD))
    cursor = db_conn.cursor()
    print('Connected to the database')

    return (db_conn,cursor)

def getLastDate(cursor,tablename):
    cursor.execute(f"SELECT MAX(date) FROM {tablename}")
    maxdate = cursor.fetchall()[0][0]

    return maxdate

def getData(state,state_cd,url,driver,current_date):
    columns = ['state', 'date', 'Cases', 'Deaths', 'Recovered']
    driver.get(url.format(state))
    element_present = EC.presence_of_element_located((By.XPATH, '//table/parent::node()'))
    table = WebDriverWait(driver, WAIT).until(element_present).get_attribute('innerHTML')
    df = pd.read_html(table)[0]
    df['state'] = state_cd
    df['date'] = pd.to_datetime(df['Date']).dt.date
    df = df[columns]
    df = df[df['date'] > current_date]

    if not df.empty:
        return df
    return None

def main():
    driver = _Chrome(driver_path)
    new_df = pd.DataFrame()
    db_conn,cursor = _Postgres(DATABASE, USER, HOST, PASSWORD)
    current_date = getLastDate(cursor,tablename)
    for state,state_cd in dict.items():
        df = getData(state,state_cd,url,driver,current_date)
        new_df = new_df.append(df, ignore_index=True)
    driver.quit()

    if not new_df.empty:
        new_df.to_csv(_file, index=False)

        ## copy
        with open(_file, 'r') as ifile:
            SQL_STATEMENT = "COPY %s (state,date,cases,death,recovered) FROM STDIN WITH CSV NULL AS '' header"
            print("Executing Copy in "+tablename)
            cursor.copy_expert(sql=SQL_STATEMENT % tablename, file=ifile)
            db_conn.commit()

        cursor.close()
        db_conn.close()
        remove(_file)

        ### VACUUM ANALYZE
        call('psql -d torkcapital -c "VACUUM ANALYZE '+tablename+'";',shell=True)
    else:
        raise ValueError('No new data')
        cursor.close()
        db_conn.close()

if __name__=="__main__":
    main()