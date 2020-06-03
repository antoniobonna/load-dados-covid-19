# -*- coding: utf-8 -*-
import scrapy
from scrapy_splash import SplashRequest
import psycopg2
import credentials
import logging
import json

logger = logging.getLogger()

class ReclameAquiSpider(scrapy.Spider):
    name = 'reclame_aqui'
    allowed_domains = ['www.reclameaqui.com.br','iosite.reclameaqui.com.br'] 

    script_urls = '''
        function main(splash, args)
            splash.private_mode_enabled = false
            url = args.url
            assert(splash:go(url))
            while not splash:select('.complain-list li a') do
                splash:wait(0.1)
              end
            splash:set_viewport_full()
            return splash:html()
        end
    '''

    script_complain = '''
        function main(splash, args)
            splash.private_mode_enabled = false
            url = args.url
            assert(splash:go(url))
            while not splash:select('.complain-body') do
                splash:wait(0.1)
              end
            splash:set_viewport_full()
            return splash:html()
        end
    '''

    def start_requests(self):
        DATABASE, HOST, USER, PASSWORD = credentials.setDatabaseLogin()
        db_conn,cursor = self._Postgres(DATABASE, USER, HOST, PASSWORD)
        query = "SELECT empresa_id FROM reclame_aqui_dw.empresa WHERE empresa_id != 'btg-pactual-digital' AND reclamacoes_avaliadas = True ORDER BY 1"
        cursor.execute(query)
        empresas = [item[0] for item in cursor.fetchall()]
        cursor.close()
        db_conn.close()
        for empresa in empresas:
            for i in range(1,5):
                # yield scrapy.Request(
                # url='https://iosite.reclameaqui.com.br/raichu-io-site-v1/complain/public/LJXlxWb42Aj87tTe',
                # callback = self.parseComplain,
                # meta = {'empresa': empresa}
                # )
                yield SplashRequest(
                    url = f"https://www.reclameaqui.com.br/empresa/{empresa}/lista-reclamacoes/?pagina={i}&status=EVALUATED",
                    callback=self.parse, 
                    endpoint="execute", 
                    args={'lua_source': self.script_urls},
                    meta = {'empresa': empresa}
                )

    def _Postgres(self, DATABASE, USER, HOST, PASSWORD):
        ### conecta no banco de dados
        db_conn = psycopg2.connect("dbname='{}' user='{}' host='{}' password='{}'".format(DATABASE, USER, HOST, PASSWORD))
        cursor = db_conn.cursor()
        return db_conn,cursor

    def parse(self, response):
        empresa = response.request.meta['empresa']
        links = response.xpath('//*[@class="complain-list"]/li/a')
        for link in links:
            id = link.xpath(".//@href").get().split('/')[-2].partition('_')[-1]
            newurl = 'https://iosite.reclameaqui.com.br/raichu-io-site-v1/complain/public/' + id
            yield scrapy.Request(
                url=newurl,
                callback = self.parseComplain,
                meta = {'empresa': empresa}
                )
            # yield SplashRequest(
                    # url = response.urljoin(link.xpath(".//@href").get()),
                    # callback = self.parseComplain, 
                    # endpoint="execute", 
                    # args={'lua_source': self.script_complain},
                    # meta = {'empresa': empresa}
                # )

    def parseComplain(self,response):
        from datetime import datetime

        d = json.loads(response.text)
        empresa = response.request.meta['empresa']
        title = d['title']
        city = d['userCity']
        state = d['userState']
        if len(state) > 2:
            state = None
        id = d['legacyId']
        current_datetime = d['created']
        current_datetime = str(datetime.strptime(d['created'],'%Y-%m-%dT%H:%M:%S'))
        complain = d['description'].replace('<br />','\n').replace('&quot;','\"')
        score = d['score']
        yield {
                'empresa': empresa,
                'titulo': title,
                'cidade': city,
                'uf': state,
                'id': id,
                'datetime': current_datetime,
                'reclamacao': complain,
                'nota' : score
           }

    # def parseComplain(self, response):
        # empresa = response.request.meta['empresa']
        # title = response.xpath('//h1/text()').get()
        # fields = response.xpath('//*[@class="local-date list-inline"]/li')
        # cidade,estado,id,current_datetime = self.getFields(fields)
        # complain = '\n'.join(response.xpath('//div[@class="complain-body"]/p/text()').extract())
        # score = response.xpath('(//*[@class="score-seal"])[3]/div[2]//p/text()').get()

        # yield{
                # 'empresa': empresa,
                # 'titulo': title,
                # 'cidade': cidade,
                # 'uf': estado,
                # 'id': id,
                # 'datetime': current_datetime,
                # 'reclamacao': complain,
                # 'nota' : score
           # }

    # def getFields(self,fields):
        # from datetime import datetime

        # cidade = fields[0].xpath('.//text()').get().split('-')[0].strip()
        # estado = fields[0].xpath('.//text()').get().split('-')[-1].strip()
        # if len(estado) > 2:
            # estado = None
        # id = fields[1].xpath('.//text()').get().split()[-1]
        # str_datetime = fields[2].xpath('.//text()').get().strip()
        # current_datetime = str(datetime.strptime(str_datetime, '%d/%m/%y às %Hh%M'))

        # return cidade,estado,id,current_datetime