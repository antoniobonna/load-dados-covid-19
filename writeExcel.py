# -*- coding: utf-8 -*-
"""
Created on Thu Nov  7 01:51:25 2019

@author: Antonio
"""

import os
from openpyxl import workbook #pip install openpyxl
from openpyxl import load_workbook
#from openpyxl.styles import NamedStyle, Font, Alignment, Color
import psycopg2
import credentials

countries = ['Brazil','United States','Italy','Spain','United Kingdom','Germany','France','Iran','Turkey','South Korea']
file = 'tabelas_email.xlsx'
indir = '/home/ubuntu/dump/dados_covid_19/'
outdir = '/home/ubuntu/scripts/load-dados-covid-19/'

query_highlights = '''SELECT "#", country, cases, total_rate, population_rate, deaths, deaths_per_million, death_rate, recovered, tests, tests_per_thousand
	FROM covid_19_dw.vw_highlights ORDER BY 1 LIMIT 20'''

query_country = '''SELECT date, cases, new_cases, cases_evolution, deaths, new_deaths, deaths_evolution, death_rate
	FROM covid_19_dw.vw_world_data
	WHERE country = '{}' 
	ORDER BY date OFFSET 1'''

DATABASE, HOST, USER, PASSWORD = credentials.setDatabaseLogin()

### conecta no banco de dados
db_conn = psycopg2.connect("dbname='{}' user='{}' host='{}' password='{}'".format(DATABASE, USER, HOST, PASSWORD))
cursor = db_conn.cursor()
print('Connected to the database')

wb = load_workbook(filename=indir+file, read_only=False)

sheet = wb['Highlights']

cursor.execute(query_highlights)
result = [item for item in cursor.fetchall()]

for i in range(len(result)):
    for j in range(len(result[i])):
        sheet.cell(row = i+2, column = j+1).value = result[i][j]

for country in countries:
    sheet = wb[country]
    cursor.execute(query_country.format(country))
    result = [item for item in cursor.fetchall()]
    for i in range(len(result)):
        for j in range(len(result[i])):
            sheet.cell(row = i+3, column = j+1).value = result[i][j]

wb.save(outdir+file)
wb.close()
cursor.close()
db_conn.close()