# -*- coding: utf-8 -*-

from selenium import webdriver
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from datetime import date, timedelta
import credentials
import psycopg2
from pyvirtualdisplay import Display
from subprocess import call
from time import sleep

### variaveis
DATABASE, HOST, USER, PASSWORD = credentials.setDatabaseLogin()
WAIT = 60
SLEEPTIME = 10
tablename = 'covid_19.local_hospitalization'
current_date = str(date.today()-timedelta(days=1))

def getText(driver):
    while True:
        try:
            texto = driver.find_elements_by_xpath('//*[starts-with(@id,"ember")]')[0].text
        except:
            pass
        else:
            return texto

display = Display(visible=0, size=(1536,864))
display.start()
driver = webdriver.Chrome(executable_path='/home/ubuntu/scripts/load-dados-reclame-aqui/chromedriver')
driver.get("https://experience.arcgis.com/experience/38efc69787a346959c931568bd9e2cc4")
driver.maximize_window()

driver.implicitly_wait(WAIT)

element_present = EC.presence_of_element_located((By.XPATH, '//*[@id="page_0"]/div/div/div/div/div/div/iframe'))
WebDriverWait(driver, WAIT).until(element_present)

iframe = driver.find_element_by_xpath('//*[@id="page_0"]/div/div/div/div/div/div/iframe')
driver.switch_to.frame(iframe)

sleep(SLEEPTIME)
# element_present = EC.presence_of_element_located((By.XPATH, '//*[starts-with(@id,"ember")]'))
# WebDriverWait(driver, WAIT).until(element_present)

texto = getText(driver)

hospitalizados_mun = texto.split('Na rede municipal:\nHospitalizados\n')[1].split('\nEm UTI')[0]
uti_mun = texto.split('\nNa rede SUS:')[0].split('Em UTI\n')[1]

hospitalizados_sus = texto.split('\nNa rede SUS:')[1].split('\nHospitalizados\n')[1].split('\n')[0]
uti_sus = texto.split('\nNa rede SUS:')[1].split('\nEm UTI\n')[1].split('\n')[0]

driver.quit()
display.stop()

### conecta no banco de dados
db_conn = psycopg2.connect("dbname='{}' user='{}' host='{}' password='{}'".format(DATABASE, USER, HOST, PASSWORD))
cursor = db_conn.cursor()

query = "INSERT INTO {} VALUES ('{}','Rio de Janeiro - RJ',{},{},{},{})".format(tablename,current_date,hospitalizados_mun,uti_mun,hospitalizados_sus,uti_sus)
# print(query)
cursor.execute(query)
db_conn.commit()

cursor.close()
db_conn.close()

### VACUUM ANALYZE
call('psql -d torkcapital -c "VACUUM ANALYZE '+tablename+'";',shell=True)