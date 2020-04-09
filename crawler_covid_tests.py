# -*- coding: utf-8 -*-

from selenium import webdriver
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from datetime import date, datetime
import credentials
import psycopg2
from pyvirtualdisplay import Display
from subprocess import call
import pickle
import os
import csv
from time import sleep

### variaveis
DATABASE, HOST, USER, PASSWORD = credentials.setDatabaseLogin()
WAIT = 60
tablename = 'covid_19.tests_stg'
indir = '/home/ubuntu/dump/dados_covid_19/'
outdir = '/home/ubuntu/scripts/load-dados-covid-19/csv/'
fcookie = '/home/ubuntu/scripts/load-dados-covid-19/cookies.pkl'
file = 'full-list-total-tests-for-covid-19.csv'
chromeOptions = webdriver.ChromeOptions()
prefs = {"download.default_directory" : indir}
chromeOptions.add_experimental_option("prefs",prefs)

def checkFile(time):
    print('Downloading file...')
    count = 0
    while count < 5:
        if os.path.isfile(indir+file):
            sleep(time)
            return
        else:
            sleep(time)
            count += 1

def downloadFile(url):
    cookies = pickle.load(open(fcookie, "rb"))
    display = Display(visible=0, size=(800,600))
    display.start()
    driver = webdriver.Chrome(executable_path='/home/ubuntu/scripts/load-dados-reclame-aqui/chromedriver', options=chromeOptions)
    driver.get(url)
    driver.implicitly_wait(WAIT)

    for cookie in cookies:
        driver.add_cookie(cookie)

    driver.refresh()

    element_present = EC.element_to_be_clickable((By.XPATH, '/html/body/main/figure/div/div[3]/div[2]/nav/ul/li[3]/a'))
    WebDriverWait(driver, WAIT).until(element_present)

    driver.find_element_by_xpath('/html/body/main/figure/div/div[3]/div[2]/nav/ul/li[3]/a').click()
    element_present = EC.element_to_be_clickable((By.XPATH, '/html/body/main/figure/div/div[4]/div/a'))
    WebDriverWait(driver, WAIT).until(element_present)
    driver.find_element_by_xpath('/html/body/main/figure/div/div[4]/div/a').click()

    checkFile(5)

    driver.quit()
    display.stop()

def loadData():
    downloadFile("https://ourworldindata.org/grapher/full-list-total-tests-for-covid-19")
    with open(indir+file, 'r', encoding="latin-1") as ifile:
        reader = csv.reader(ifile, delimiter=',')
        header = next(reader, None)  ### Pula o cabeçalho
        with open(outdir+file,'w', newline="\n", encoding="utf-8") as ofile:
            writer = csv.writer(ofile, delimiter=';')
            for row in reader:
                if (row[0] == 'United States - specimens tested (CDC)') or row[0] == 'India - people tested':
                    continue
                row[0] = row[0].split(' - ')[0]
                del row[1]
                row[1] = str(datetime.strptime(row[1], '%b %d, %Y').date())
                writer.writerow(row)
    os.remove(indir+file)

    ### conecta no banco de dados
    db_conn = psycopg2.connect("dbname='{}' user='{}' host='{}' password='{}'".format(DATABASE, USER, HOST, PASSWORD))
    cursor = db_conn.cursor()

    ### truncate
    cursor.execute("TRUNCATE "+tablename)
    db_conn.commit()

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

loadData()