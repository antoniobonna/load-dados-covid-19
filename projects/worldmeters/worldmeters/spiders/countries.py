# -*- coding: utf-8 -*-
import scrapy


class CountriesSpider(scrapy.Spider):
    name = 'countries'
    allowed_domains = ['www.worldometers.info']
    start_urls = ['https://www.worldometers.info/world-population/population-by-country/']

    def parse(self, response):
        #title = response.xpath("/h1/text()").get()
        countries = response.css("td a") ###.xpath('//td/a')
        for country in countries:
            name = country.xpath('.//text()').get()
            link = country.xpath('.//@href').get()
            
            #absolute_url = response.urljoin(link)
            
            #yield scrapy.Request(url=absolute_url)
            yield response.follow(url=link, callback = self.parseCountry, meta = {'country_name': name})

    def parseCountry(self, response):
        name = response.request.meta['country_name']
        rows = response.xpath("(//table[@class='table table-striped table-bordered table-hover table-condensed table-list'])[1]/tbody/tr")
        for row in rows:
            year = row.xpath('.//td[1]/text()')
            population = row.xpath('.//td[2]/strong/text()').get()
            
            yield{
                'name': name,
                'year': year,
                'population': population
           }