# -*- coding: utf-8 -*-
import scrapy
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
from locale import setlocale,LC_TIME
from datetime import datetime,date
import re
import csv

class HospitalizationSpider(CrawlSpider):
    name = 'hospitalization'
    allowed_domains = ['www.fvs.am.gov.br']
    # start_urls = [f'http://www.fvs.am.gov.br/noticias?page={i}#' for i in range(1,9)]
    start_urls = ['http://www.fvs.am.gov.br/noticias']

    rules = (
        #Rule(LinkExtractor(restrict_xpaths='//h3[@class="blog-title"]/a'), callback='parse_item', follow=True),
        Rule(LinkExtractor(restrict_xpaths='//table/tbody/tr/td[2]/a'), callback='parse_item', follow=True),
        #Rule(LinkExtractor(restrict_xpaths='//a[@class="next page-numbers"]')),
    )
    
    def parseDate(self, str_date):
        setlocale(LC_TIME, 'pt_BR')
        match = re.search(r'\d+ de \w+ de \d{4}',str_date).group()
        _date = datetime.strptime(match,'%d de %B de %Y').date()

        return str(_date)

    def parse_rows(self, selector):
        # TAG_CASES = ['casos confirmados','leitos clínicos','uti']
        # TAG_CASES2 = ['casos positivos','leitos clínicos','uti']
        # TAG_SUSPECTS = ['pacientes suspeitos','leitos clínicos','uti']
        TAG_OCCUPATION_ICU = ['taxa de ocupação','uti']
        TAG_OCCUPATION_NURSERY = ['taxa de ocupação','leitos clínicos']

        nursery_public = icu_public = nursery_sus = icu_sus = icu_beds = nursery_beds = None
        rows = [t.replace('(','').replace(')','').lower() for sublist in selector for t in sublist.xpath('.//text()').extract() if t != '\xa0']
        for i,row in enumerate(rows):
            if row.startswith('internações'):
                if 'casos confirmados' in rows[i+1] or 'casos positivos' in rows[i+1]:
                    cases = [int(x.replace('.','')) for x in rows[i+1].split() if x.replace('.','').isdigit()]
                    suspects = [int(x.replace('.','')) for x in rows[i+2].split() if x.replace('.','').isdigit()]
                    nursery_public = cases[3] + suspects[3]
                    icu_public = cases[-1] + suspects[-1]
                else:
                    cases = [int(x.replace('.','')) for x in rows[i].split() if x.replace('.','').isdigit()]
                    suspects = [int(x.replace('.','')) for x in rows[i].split() if x.replace('.','').isdigit()]
                    nursery_public = cases[3] + suspects[3]
                    icu_public = cases[-1] + suspects[-1]
                nursery_public += icu_public
                # return (nursery_public+icu_public,icu_public)

            # if all(w in row for w in TAG_CASES) or all(w in row for w in TAG_CASES2):
                # if re.findall(r'.a rede pública \d+',row):
                    # cases = [int(x) for x in ' '.join(re.findall(r'.a rede pública \d+',row)).split() if x.isdigit()]
                # else:
                    # cases = [int(x) for x in ' '.join(re.findall(r'\d+ .a rede pública',row)).split() if x.isdigit()]
                # icu_public = cases[0]
            # if all(w in row for w in TAG_SUSPECTS):
                # if re.findall(r'.a rede pública \d+',row):
                    # suspects = [int(x) for x in ' '.join(re.findall(r'.a rede pública \d+',row)).split() if x.isdigit()]
                # else:
                    # suspects = [int(x) for x in ' '.join(re.findall(r'\d+ .a rede pública',row)).split() if x.isdigit()]
                # icu_public += suspects[-1]
                
            # elif row.startswith('ocupação de leitos'):
                # if all(w in rows[i+1] for w in TAG_OCCUPATION_ICU):
                    # icu_beds = [int(x) for x in re.search(r'\d+(\.\d+)? leitos de uti',rows[i+1]).group().split() if x.isdigit()][0]
                    # icu_sus = [int(x) for x in re.search(r'\d+ estavam ocupados',rows[i+1]).group().split() if x.isdigit()][0]
                # elif all(w in rows[i] for w in TAG_OCCUPATION_ICU):
                    # icu_beds = [int(x) for x in re.search(r'\d+(\.\d+)? leitos de uti',rows[i]).group().split() if x.isdigit()][0]
                    # icu_sus = [int(x) for x in re.search(r'\d+ estavam ocupados',rows[i]).group().split() if x.isdigit()][0]
                # if all(w in rows[i+2] for w in TAG_OCCUPATION_NURSERY):
                    # nursery_beds = [int(x.replace('.','')) for x in re.search(r'\d+\.\d+ leitos disponíveis',rows[i+2]).group().split() if x.replace('.','').isdigit()][0]
                    # nursery_sus = [int(x.replace('.','')) for x in re.search(r'\d+\.\d+ estavam ocupados',rows[i+2]).group().split() if x.replace('.','').isdigit()][0]
                # elif all(w in rows[i+1] for w in TAG_OCCUPATION_NURSERY):
                    # nursery_beds = [int(x.replace('.','')) for x in re.search(r'\d+\.\d+ leitos disponíveis',rows[i+1]).group().split() if x.replace('.','').isdigit()][0]
                    # nursery_sus = [int(x.replace('.','')) for x in re.search(r'\d+\.\d+ estavam ocupados',rows[i+1]).group().split() if x.replace('.','').isdigit()][0]
                # nursery_sus += icu_sus

        if icu_public:
            return (nursery_public,icu_public,nursery_sus,icu_sus,icu_beds,nursery_beds)
        return None

    def parse_item(self, response):
        hospitalization_columns = ['date','local','inpatients','icu','inpatients_sus','icu_sus','queue','icu_queue']
        bed_columns = ['date','local','bed','bed_number']
        result = None
        _date = response.xpath('//p[@class="text-subtit"]//small/text()').get()
        _date = self.parseDate(_date)
        #if _date >= '2020-04-10':
        rows = response.xpath('//div[@class="col-md-12"][1]/p')
        result = self.parse_rows(rows)

        if result:
            inpatients,icu,inpatients_sus,icu_sus,icu_beds,nursery_beds = result
            local_hospitalization = {
                'date': _date,
                'local': 'Amazonas',
                'inpatients': inpatients,
                'icu': icu,
                'inpatients_sus': inpatients_sus,
                'icu_sus': icu_sus,
            }
            with open('hospitalization.csv','a', newline="\n", encoding="utf-8") as ofile:
                writer = csv.DictWriter(ofile, fieldnames=hospitalization_columns,restval='', extrasaction='ignore')
                writer.writerow(local_hospitalization)

            if icu_sus:
                local_beds = {
                'date': _date,
                'local': 'Amazonas',
                'bed': 'ICU SUS',
                'bed_number': icu_beds
                }
                with open('beds.csv','a', newline="\n", encoding="utf-8") as ofile:
                    writer = csv.DictWriter(ofile, fieldnames=bed_columns,restval='', extrasaction='ignore')
                    writer.writerow(local_beds)
