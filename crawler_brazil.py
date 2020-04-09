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

def removeOldFiles():
    oldfiles = [f for f in os.listdir(indir) if f.endswith('.csv')]
    for oldfile in oldfiles:
        os.remove(indir+oldfile)

def checkFile(time):
    print('Downloading file...')
    count = 0
    while count < 5:
        try:
            file = [f for f in os.listdir(indir) if f.endswith('.csv')][0]
        except:
            sleep(time)
            count += 1
        else:
            sleep(time)
            return file

def convertDate(data):
    try:
        newdate = datetime.strptime(data,'%d/%m/%Y').date()
    except:
        newdate = datetime.fromordinal(datetime(1900, 1, 1).toordinal() + int(data) - 2).date()
    return newdate

def downloadFile(url):
    removeOldFiles()

    chromeOptions = webdriver.ChromeOptions()
    prefs = {"download.default_directory" : indir}
    chromeOptions.add_experimental_option("prefs",prefs)
    display = Display(visible=0, size=(800,600))
    display.start()
    driver = webdriver.Chrome(executable_path='/home/ubuntu/scripts/load-dados-reclame-aqui/chromedriver', options=chromeOptions)
    driver.get(url)

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
    return file

def parseCSV():
    file = downloadFile("https://covid.saude.gov.br/")

    with open(indir+file, 'r', encoding="latin-1") as ifile:
        reader = csv.reader(ifile, delimiter=';')
        header = next(reader, None)  ### Pula o cabeçalho
        with open(outdir+file,'w', newline="\n", encoding="utf-8") as ofile:
            writer = csv.writer(ofile, delimiter=';')
            for row in reader:
                if convertDate(row[2]) != current_date:
                    continue
                row[2] = str(convertDate(row[2]))
                newrow = [row[2],row[1],row[4],row[6]]
                writer.writerow(newrow)
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

#parseCSV()