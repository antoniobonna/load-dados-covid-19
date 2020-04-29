# -*- coding: utf-8 -*-

from datetime import date,datetime
import csv
import requests

### variaveis
outdir = '/home/ubuntu/scripts/load-dados-covid-19/csv/'
file_deaths = 'deaths_cities_us.csv'
file_cases = 'cases_cities_us.csv'

DEATHS_CSV_URL = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_US.csv'
CASES_CSV_URL = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_US.csv'

with requests.get(DEATHS_CSV_URL, stream=True) as r:
    lines = (line.decode('utf-8') for line in r.iter_lines())
    reader = csv.DictReader(lines)
    with open(outdir+file_deaths,'w', newline="\n", encoding="utf-8") as ofile:
        writer = csv.writer(ofile, delimiter=';')
        for row in reader:
            if row['Admin2'] and row['Admin2'] != 'Unassigned' and row['FIPS'] and not row['Admin2'].startswith('Out of'):
                for key, value in row.items():
                    if key.endswith('/20'):
                        new_row = [str(datetime.strptime(key, '%m/%d/%y').date()), row['FIPS'].split('.')[0], row['Admin2'], row['Province_State'],row[key]]
                        writer.writerow(new_row)

with requests.get(CASES_CSV_URL, stream=True) as r:
    lines = (line.decode('utf-8') for line in r.iter_lines())
    reader = csv.DictReader(lines)
    with open(outdir+file_cases,'w', newline="\n", encoding="utf-8") as ofile:
        writer = csv.writer(ofile, delimiter=';')
        for row in reader:
            if row['Admin2'] and row['Admin2'] != 'Unassigned' and row['FIPS'] and not row['Admin2'].startswith('Out of'):
                for key, value in row.items():
                    if key.endswith('/20'):
                        new_row = [str(datetime.strptime(key, '%m/%d/%y').date()), row['FIPS'].split('.')[0], row['Admin2'], row['Province_State'],row[key]]
                        writer.writerow(new_row)