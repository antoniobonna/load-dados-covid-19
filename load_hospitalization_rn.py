# -*- coding: utf-8 -*-

from datetime import datetime, date
import credentials
import psycopg2
from subprocess import call
from selenium import webdriver
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.options import Options
import re

### variaveis
DATABASE, HOST, USER, PASSWORD = credentials.setDatabaseLogin()
table_icu = 'covid_19.local_hospitalization'
table_beds = 'covid_19.local_beds'
state_local = 'Rio Grande do Norte'
current_date = date.today()
url_main = 'https://covid.lais.ufrn.br/'
url_beds = 'https://regulacao.lais.ufrn.br/sala-situacao/sala_publica/'
WAIT = 60
driver_path = '/home/ubuntu/scripts/load-dados-reclame-aqui/chromedriver'

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

def getDate(driver,wait):
    element_present = EC.visibility_of_element_located((By.ID, 'row'))
    WebDriverWait(driver, wait).until(element_present)
    str_date = driver.find_element_by_xpath('//*[contains(text(),"Última atualização")]').text
    match = re.search(r'\d{2}\/\d{2}\/\d{4}',str_date).group()
    last_date = datetime.strptime(match, '%d/%m/%Y').date()

    return last_date

def parsePage(url):
    import requests
    from parsel import Selector
    # import pandas as pd

    page = requests.get(url)
    selector = Selector(text=page.text)
    total_beds = int(selector.xpath('//p[text() ="Leitos Críticos COVID-19"]/parent::node()/h2//text()').get())
    lock_beds = int(selector.xpath('//h4[text() ="Bloqueado"]/parent::node()/h3//text()').get())
    icu_beds = total_beds - lock_beds
    icu = selector.xpath('//h4[text() ="Ocupado"]/parent::node()/h3//text()').get()
    
    # table = selector.css('table#tabela-ocupacao').get()
    # df = pd.read_html(table)[0]
    # icu_beds = (df['Total'] - df['Bloq.']).sum()
    # icu = (df['UTI Ocup.'] + df['UCI Ocup.']).sum()

    return (icu,icu_beds)

def insertDB(db_conn,cursor,query):
    print(query)
    cursor.execute(query)
    db_conn.commit()

def main():
    driver = _Chrome(driver_path)
    driver.get(url_main)
    last_date = getDate(driver,WAIT)
    driver.quit()
    if last_date == current_date:

        icu, icu_beds = parsePage(url_beds)

        db_conn,cursor = _Postgres(DATABASE, USER, HOST, PASSWORD)
        query = f"INSERT INTO {table_icu} VALUES ('{str(current_date)}','{state_local}',NULL,{icu})"
        insertDB(db_conn,cursor,query)

        query = f"INSERT INTO {table_beds} VALUES ('{str(current_date)}','{state_local}','ICU',{icu_beds})"
        insertDB(db_conn,cursor,query)

        cursor.close()
        db_conn.close()

        ### VACUUM ANALYZE
        call('psql -d torkcapital -c "VACUUM ANALYZE '+table_icu+'";',shell=True)
        call('psql -d torkcapital -c "VACUUM ANALYZE '+table_beds+'";',shell=True)

    else:
        raise ValueError('No data for ' + str(current_date))

if __name__=="__main__":
    main()