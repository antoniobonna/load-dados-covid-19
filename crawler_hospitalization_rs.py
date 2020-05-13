# -*- coding: utf-8 -*-

from bs4 import BeautifulSoup as bs
from selenium import webdriver
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.options import Options
from datetime import date, timedelta
import credentials
import psycopg2
from subprocess import call
from locale import setlocale,LC_TIME

### variaveis
DATABASE, HOST, USER, PASSWORD = credentials.setDatabaseLogin()
WAIT = 60
tablenameh = 'covid_19.local_hospitalization'
tablenameb = 'covid_19.local_beds'
current_date = date.today()
setlocale(LC_TIME, 'pt_BR')
str_date = current_date.strftime('%d de %B')
driver_path = '/home/ubuntu/scripts/load-dados-reclame-aqui/chromedriver'
url = "https://covid.saude.rs.gov.br/"
local = 'Rio Grande do Sul'

def _Chrome(driver_path):
    chrome_options = Options()
    chrome_options.add_argument("--headless")
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

def parsePage(driver,WAIT):
    element_present = EC.presence_of_element_located((By.XPATH, '//app-beds-amount'))
    WebDriverWait(driver, WAIT).until(element_present)
    bs_page = bs(driver.page_source, 'html.parser')
    return bs_page

def getDate(bs_page):
    strdate = bs_page.find('p', {'class': 'update'}).text.split(':')[1].split('às')[0].strip()
    return strdate

def getValues(beds):
    icu_beds = int(beds[0].find('span', {'class': 'number'}).text)
    icu = int(beds[0].find('div', {'class': 'badget'}).text.split()[0])
    bed = int(beds[1].find('span', {'class': 'number'}).text)
    inpacients = int(beds[1].find('div', {'class': 'badget'}).text.split()[0]) + icu
    return (bed,icu_beds,inpacients,icu)

def insertDB(db_conn,cursor,query):
    print(query)
    cursor.execute(query)
    db_conn.commit()

def main():
    driver = _Chrome(driver_path)
    driver.get(url)

    bs_page = parsePage(driver,WAIT)
    driver.quit()

    if str_date == getDate(bs_page):

        boxes = bs_page.find_all('div', {'class': 'bed'})
        bed, icu_beds, inpacients, icu = getValues(boxes)

        db_conn,cursor = _Postgres(DATABASE, USER, HOST, PASSWORD)
        query = f"INSERT INTO {tablenameh} VALUES ('{str(current_date)}','{local}',{inpacients},{icu})"
        insertDB(db_conn,cursor,query)
        query = f"INSERT INTO {tablenameb} VALUES ('{str(current_date)}','{local}','ICU',{icu_beds})"
        insertDB(db_conn,cursor,query)
        query = f"INSERT INTO {tablenameb} VALUES ('{str(current_date)}','{local}','Nursery',{bed})"
        insertDB(db_conn,cursor,query)

        cursor.close()
        db_conn.close()

        ### VACUUM ANALYZE
        call('psql -d torkcapital -c "VACUUM ANALYZE '+tablenameh+'";',shell=True)
        call('psql -d torkcapital -c "VACUUM ANALYZE '+tablenameb+'";',shell=True)

    else:
        raise ValueError('No data for ' + str(current_date))

if __name__=="__main__":
    main()