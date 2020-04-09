# -*- coding: utf-8 -*-

import load_dados_ministerio
import crawler_brazil

try:
    load_dados_ministerio.checkNewData('https://www.saude.gov.br/noticias')
except:
    print('Error in "Ministerio da Saúde"')
    crawler_brazil.parseCSV()