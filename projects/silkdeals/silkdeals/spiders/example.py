# -*- coding: utf-8 -*-
import scrapy
from scrapy_selenium import SeleniumRequest
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
import csv
import psycopg2
import credentials

class ExampleSpider(scrapy.Spider):
    name = 'example'
    # allowed_domains = ['example.com']
    # start_urls = ['http://example.com/']

    def start_requests(self):
        DATABASE, HOST, USER, PASSWORD = credentials.setDatabaseLogin()
        db_conn,cursor = self._Postgres(DATABASE, USER, HOST, PASSWORD)
        query = "SELECT empresa_id FROM reclame_aqui_dw.empresa WHERE empresa_id != 'btg-pactual-digital' AND reclamacoes_avaliadas = True ORDER BY 1"
        cursor.execute(query)
        empresas = [item[0] for item in cursor.fetchall()]
        cursor.close()
        db_conn.close()
        for empresa in empresas:
            for i in range(1,6):
                yield SeleniumRequest(
                    url = f"https://www.reclameaqui.com.br/empresa/{empresa}/lista-reclamacoes/?pagina={i}&status=EVALUATED",
                    wait_time=60,
                    wait_until=EC.presence_of_element_located((By.CLASS_NAME,'complain-list')),
                    callback=self.parse,
                    meta = {'empresa': empresa}
                )

    def _Postgres(self, DATABASE, USER, HOST, PASSWORD):
        ### conecta no banco de dados
        db_conn = psycopg2.connect("dbname='{}' user='{}' host='{}' password='{}'".format(DATABASE, USER, HOST, PASSWORD))
        cursor = db_conn.cursor()
        return db_conn,cursor

    def parse(self, response):
        #driver = response.request.meta['driver']
        
        #html = driver.page_source
        #response_obj = Selector(text=html)
        
        #driver.save_screenshot('enter.png')
        #links = response_obj.xpath('//*[@class="complain-list"]/li/a')
        
        
        empresa = response.request.meta['empresa']
        links = response.selector.xpath('//*[@class="complain-list"]/li/a')
        for link in links:
            yield {
                'empresa' : empresa,
                'URL' : response.urljoin(link.xpath(".//@href").get())
            }
