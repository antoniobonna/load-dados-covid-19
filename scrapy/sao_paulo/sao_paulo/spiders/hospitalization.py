# -*- coding: utf-8 -*-
import scrapy
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
from datetime import datetime,date
import csv
import re


class HospitalizationSpider(CrawlSpider):
    name = 'hospitalization'
    allowed_domains = ['www.saopaulo.sp.gov.br']
    start_urls = ['https://www.saopaulo.sp.gov.br/noticias-coronavirus']

    rules = (
        Rule(LinkExtractor(restrict_xpaths='//h3[@class="title"]/a'), callback='parse_item', follow=True),
        #Rule(LinkExtractor(restrict_xpaths='//a[@class="next page-numbers"]')),
    )

    def parseDate(self, str_date):
        match = re.search(r'\d{2}\/\d{2}\/\d{4}',str_date).group()
        last_date = datetime.strptime(match, '%d/%m/%Y').date()

        return str(last_date)

    def parse_rows(self, selector):
        TAG_INPATIENTS = ['internad','uti','enfermaria']
        TAG_INPATIENTS2 = ['internaç','uti','enfermaria']
        TAG_OCCUPATION = ['taxa de ocupação','uti','estado']
        icu = occupation = []

        rows = [t.replace(' %','%').replace('%,','%').replace('%.','%').replace('taxas','taxa').lower() for sublist in selector for t in sublist.xpath('.//text()').extract()]
        for i,row in enumerate(rows):
            if all(w in row for w in TAG_INPATIENTS) or all(w in row for w in TAG_INPATIENTS2):
                try:
                    icu = [int(x.replace('.','')) for x in re.search(r'\d+\.\d+ em uti',row).group().split() if x.replace('.','').isdigit()][0]
                except:
                    icu = [int(x.replace('.','')) for x in re.search(r'\d+\.\d+ em unidades de terapia intensiva',row).group().split() if x.replace('.','').isdigit()][0]
                nursery = [int(x.replace('.','')) for x in re.search(r'\d+\.\d+ em enfermaria',row).group().split() if x.replace('.','').isdigit()][0]
            if all(w in row for w in TAG_OCCUPATION):
                try:
                    occupation = [float(x.replace(',','.').replace('%','')) for x in row.split() if x.endswith('%')]
                    if occupation[0] < occupation[1]:
                        icu_rate_state = occupation[0]
                    else:
                        icu_rate_state = occupation[1]
                except:
                    for j in range(1,len(rows) - i):
                        occupation = [float(x.replace(',','.').replace('%','')) for x in rows[i+j].split() if x.endswith('%')]
                        if len(occupation) == 2:
                            if occupation[0] < occupation[1]:
                                icu_rate_state = occupation[0]
                            else:
                                icu_rate_state = occupation[1]
                            break
        if icu and occupation:
            return (icu,nursery,icu_rate_state)
        return None

    def parse_item(self, response):
        _date = response.xpath('//header[@class="article-header"]//span[@class="date"]/text()').get()
        _date = self.parseDate(_date)
        hospitalization_columns = ['date','local','inpatients','icu','inpatients_sus','icu_sus','queue','icu_queue']
        bed_columns = ['date','local','bed','bed_number']

        rows = response.xpath('//article[@class="article-main"]/p')
        result = self.parse_rows(rows)

        if result:
            icu,nursery,icu_rate = result
            icu_beds = round(icu/icu_rate * 100)

            local_hospitalization = {
                'date': _date,
                'local': 'São Paulo',
                'inpatients': nursery + icu,
                'icu': icu
            }
            with open('hospitalization.csv','a', newline="\n", encoding="utf-8") as ofile:
                writer = csv.DictWriter(ofile, fieldnames=hospitalization_columns,restval='', extrasaction='ignore')
                writer.writerow(local_hospitalization)

            local_beds = {
                'date': _date,
                'local': 'São Paulo',
                'bed': 'ICU',
                'bed_number': icu_beds
            }
            with open('beds.csv','a', newline="\n", encoding="utf-8") as ofile:
                writer = csv.DictWriter(ofile, fieldnames=bed_columns,restval='', extrasaction='ignore')
                writer.writerow(local_beds)