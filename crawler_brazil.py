# -*- coding: utf-8 -*-

from selenium import webdriver
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.keys import Keys
from datetime import date, datetime, timedelta
import credentials
import psycopg2
from pyvirtualdisplay import Display
from subprocess import call
import os
import csv
from time import sleep

### variaveis
DATABASE, HOST, USER, PASSWORD = credentials.setDatabaseLogin()
WAIT = 60
tablename = 'covid_19.brazil_stg'
indir = '/home/ubuntu/dump/dados_covid_19/brazil/'
outdir = '/home/ubuntu/scripts/load-dados-covid-19/csv/'
current_date = date.today()-timedelta(days=1)
current_date = date.today()

def checkFile(time):
    print('Downloading file...')
    while True:
        try:
            file = [f for f in os.listdir(indir) if f.endswith('.csv')][0]
        except:
            sleep(time)
        else:
            sleep(time)
            return file

chromeOptions = webdriver.ChromeOptions()
prefs = {"download.default_directory" : indir}
chromeOptions.add_experimental_option("prefs",prefs)
display = Display(visible=0, size=(800,600))
display.start()
driver = webdriver.Chrome(executable_path='/home/ubuntu/scripts/load-dados-reclame-aqui/chromedriver', options=chromeOptions)
driver.get("https://covid.saude.gov.br/")

driver.implicitly_wait(WAIT)

element_present = EC.element_to_be_clickable((By.XPATH, '/html/body/app-root/ion-app/ion-router-outlet/app-home/ion-content'))
WebDriverWait(driver, WAIT).until(element_present)
driver.find_element_by_xpath('/html/body/app-root/ion-app/ion-router-outlet/app-home/ion-content').click()

driver.find_element_by_tag_name('html').send_keys(Keys.END)
element_present = EC.element_to_be_clickable((By.XPATH, '/html/body/app-root/ion-app/ion-router-outlet/app-home/ion-content/div[6]/div[1]/img'))
WebDriverWait(driver, WAIT).until(element_present)
driver.find_element_by_xpath('/html/body/app-root/ion-app/ion-router-outlet/app-home/ion-content/div[6]/div[1]/img').click()

file = checkFile(5)

driver.quit()
display.stop()

with open(indir+file, 'r', encoding="utf-8") as ifile:
    reader = csv.reader(ifile, delimiter=';')
    header = next(reader, None)  ### Pula o cabeçalho
    with open(outdir+file,'w', newline="\n", encoding="utf-8") as ofile:
        writer = csv.writer(ofile, delimiter=';')
        for row in reader:
            if datetime.strptime(row[2],'%d/%m/%Y').date() != current_date:
                continue
            row[2] = str(datetime.strptime(row[2], '%d/%m/%Y').date())
            del row[0]
            del row[2]
            del row[3]
            writer.writerow(row)
os.remove(indir+file)

### conecta no banco de dados
db_conn = psycopg2.connect("dbname='{}' user='{}' host='{}' password='{}'".format(DATABASE, USER, HOST, PASSWORD))
cursor = db_conn.cursor()

### copy
with open(outdir+file, 'r') as ifile:
    SQL_STATEMENT = "COPY %s FROM STDIN WITH CSV DELIMITER AS ';' NULL AS ''"
    print("Executing Copy in "+tablename)
    cursor.copy_expert(sql=SQL_STATEMENT % tablename, file=ifile)
    db_conn.commit()

os.remove(outdir+file)
cursor.close()
db_conn.close()

### VACUUM ANALYZE
call('psql -d torkcapital -c "VACUUM ANALYZE '+tablename+'";',shell=True)