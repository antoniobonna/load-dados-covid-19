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
from time import sleep

### variaveis
DATABASE, HOST, USER, PASSWORD = credentials.setDatabaseLogin()
WAIT = 60
SLEEPTIME = 10
tablename = 'covid_19.local_hospitalization'
current_date = date.today()-timedelta(days=1)
str_date = current_date.strftime('%d/%m/%Y')
driver_path = '/home/ubuntu/scripts/load-dados-reclame-aqui/chromedriver'
url = "https://experience.arcgis.com/experience/38efc69787a346959c931568bd9e2cc4"

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

def parsePage(driver,WAIT,stime):
    element_present = EC.presence_of_element_located((By.XPATH, '//iframe'))
    WebDriverWait(driver, WAIT).until(element_present)
    #iframe = driver.find_element_by_xpath('//*[@id="page_0"]/div/div/div/div/div/div/iframe')
    iframe = driver.find_element_by_xpath('//iframe')
    driver.switch_to.frame(iframe)
    sleep(stime)
    bs_page = bs(driver.page_source, 'html.parser')
    return bs_page

def getDate(bs_page):
    strdate = bs_page.find('div', {'class': 'subtitle text-ellipsis no-pointer-events'}).text.split()[2]
    return strdate

def getValuesText(boxes,key):
    for i,box in enumerate(boxes):
        if box.text.strip() == key:
            return boxes[i+1].text.strip().replace('.','')

def main():
    driver = _Chrome(driver_path)
    driver.get(url)

    bs_page = parsePage(driver,WAIT,SLEEPTIME)
    driver.quit()

    if str_date == getDate(bs_page):

        boxes = bs_page.find_all('g', {'class': 'responsive-text-label'})

        hospitalizados_mun = getValuesText(boxes,'Hospitalizados:')
        uti_mun = getValuesText(boxes,'Em UTI:')
        
        hospitalizados_sus = getValuesText(boxes,'Hospitalizados (rede SUS)')
        uti_sus = getValuesText(boxes,'Em UTI (rede SUS)')

        db_conn,cursor = _Postgres(DATABASE, USER, HOST, PASSWORD)
        query = f"INSERT INTO {tablename} VALUES ('{str(current_date)}','Rio de Janeiro - RJ',{hospitalizados_mun},{uti_mun},{hospitalizados_sus},{uti_sus})"
        print(query)
        cursor.execute(query)
        db_conn.commit()

        cursor.close()
        db_conn.close()

        ### VACUUM ANALYZE
        call('psql -d torkcapital -c "VACUUM ANALYZE '+tablename+'";',shell=True)

    else:
        raise ValueError('No data for ' + str_date)

if __name__=="__main__":
    main()