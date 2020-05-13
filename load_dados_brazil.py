# -*- coding: utf-8 -*-

import load_dados_covid19br
import crawler_brazil

CSV_URL = 'https://raw.githubusercontent.com/wcota/covid19br/master/cases-brazil-states.csv'
PAINEL_URL = "https://covid.saude.gov.br/"

try:
    load_dados_covid19br.loadData(CSV_URL)
except:
    print('Error in "COVID-19 BR"')
    crawler_brazil.parseCSV(PAINEL_URL)