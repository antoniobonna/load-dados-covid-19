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

### variaveis
DATABASE, HOST, USER, PASSWORD = credentials.setDatabaseLogin()
WAIT = 60
tablename = 'covid_19.italy_stg'
current_date = str(date.today()-timedelta(days=1))

display = Display(visible=0, size=(800,600))
display.start()
driver = webdriver.Chrome(executable_path='/home/ubuntu/scripts/load-dados-reclame-aqui/chromedriver')
driver.get("https://lab24.ilsole24ore.com/coronavirus/")

driver.implicitly_wait(WAIT)

element_present = EC.frame_to_be_available_and_switch_to_it((By.XPATH, '//*[@id="grafico-5-regioni"]'))
WebDriverWait(driver, WAIT).until(element_present)

grafico_name = []
grafico_value = []

for i in range(1,6):
    driver.switch_to.default_content()
    driver.switch_to.frame("grafico-5-regioni")

    driver.switch_to.frame("grafico_"+str(i))

    grafico_name.append(driver.find_element_by_xpath('//*[@id="text_container"]/p[1]').text.replace('-',' ').title())
    grafico_value.append(driver.find_element_by_xpath('//*[@id="text_container"]/p[2]').text.replace('.',''))

driver.switch_to.default_content()

driver.quit()
display.stop()

### conecta no banco de dados
db_conn = psycopg2.connect("dbname='{}' user='{}' host='{}' password='{}'".format(DATABASE, USER, HOST, PASSWORD))
cursor = db_conn.cursor()

for name,value in zip(grafico_name,grafico_value):
    query = "INSERT INTO {} VALUES ('{}','{}',{})".format(tablename,current_date,name,value)
    cursor.execute(query)
    db_conn.commit()

cursor.close()
db_conn.close()

### VACUUM ANALYZE
call('psql -d torkcapital -c "VACUUM ANALYZE '+tablename+'";',shell=True)