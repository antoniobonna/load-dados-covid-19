# -*- coding: utf-8 -*-

import credentials
from subprocess import call
from sqlalchemy import create_engine
import pandas as pd
from selenium import webdriver
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.options import Options

### variaveis
DATABASE, HOST, USER, PASSWORD = credentials.setDatabaseLogin()
engine = create_engine("postgresql+psycopg2://{}:{}@{}/{}".format(USER,PASSWORD,HOST,DATABASE))
schema = 'covid_19'
table_name = 'contagion_rate'
driver_path = '/home/ubuntu/scripts/load-dados-reclame-aqui/chromedriver'
url = 'https://covid19analytics.com.br/painel-de-resultados/'
WAIT = 60

def _Chrome(driver_path):
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("start-maximized")
    driver = webdriver.Chrome(executable_path=driver_path, options=chrome_options)
    return driver

def findLink(driver,WAIT):
    from time import sleep
    sleep(5)

    element_present = EC.frame_to_be_available_and_switch_to_it((By.XPATH, '//iframe'))
    WebDriverWait(driver, WAIT).until(element_present)
    element_present = EC.visibility_of_element_located((By.XPATH, '//a[@data-value="Num. de Reproducao"]'))
    tab = WebDriverWait(driver, WAIT).until(element_present).click()
    sleep(5)
    element_present = EC.visibility_of_element_located((By.XPATH, '//a[@id="download_planilha_Rts"]'))
    link = WebDriverWait(driver, WAIT).until(element_present).get_attribute('href')

    return link

def parseDF(df):
    new_df = pd.DataFrame()
    for idx, row in df.iterrows():
        for column in list(df)[1:]:
            new_row = {
                'date': row['Data'],
                'uf': column.replace('Brasil','Brazil').replace('_',' '),
                'rt': row[column]
                }
            new_df = new_df.append(new_row, ignore_index=True)
    new_df = new_df[['date','uf','rt']]

    return new_df

def parseBrazilDF(df):
    new_df = pd.DataFrame()
    for idx, row in df.iterrows(): 
        for column in list(df)[1:]:
            if column.startswith('Brasil_IDH'):
                new_row = {
                    'date': row['Data'],
                    'uf': column.replace('Brasil','Brazil').replace('_',' '),
                    'rt': row[column]
                }
                new_df = new_df.append(new_row, ignore_index=True)
    new_df = new_df[['date','uf','rt']]

    return new_df

def main():
    driver = _Chrome(driver_path)
    driver.get(url)
    csv_link = findLink(driver,WAIT)
    driver.quit()

    db_conn = engine.raw_connection()
    cursor = db_conn.cursor()
    df = pd.read_excel(csv_link, sheet_name = 'Com previsao')
    brazil_df = pd.read_excel(csv_link, sheet_name = 'Semana anterior')
    df = parseDF(df)
    #brazil_df = parseBrazilDF(brazil_df)

    cursor.execute("TRUNCATE {}.{}".format(schema,table_name))
    db_conn.commit()
    df.to_sql(table_name,engine,schema=schema,index=False,if_exists='append')
    #brazil_df.to_sql(table_name,engine,schema=schema,index=False,if_exists='append')
    cursor.close()
    db_conn.close()

    ### VACUUM ANALYZE
    call('psql -d torkcapital -c "VACUUM ANALYZE {}.{}"'.format(schema,table_name),shell=True)

if __name__=="__main__":
    main()