# -*- coding: utf-8 -*-
import scrapy
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
from scrapy.exporters import CsvItemExporter

class MyCsvItemExporter(CsvItemExporter):

    def __init__(self, file, include_headers_line=True, join_multivalued=',', **kwargs):
        kwargs['delimiter'] = ';'
        kwargs['quotechar'] = '"'
        kwargs['encoding'] = 'utf-8'
        kwargs['lineterminator'] = '\n\n'
        super().__init__(file, include_headers_line, join_multivalued, **kwargs)

class BestMoviesSpider(CrawlSpider):
    name = 'best_movies'
    allowed_domains = ['imdb.com']
    start_urls = ['https://www.imdb.com/search/title/?groups=top_250&sort=user_rating']

    rules = (
        Rule(LinkExtractor(restrict_xpaths='//h3[@class="lister-item-header"]/a'), callback='parse_item', follow=True),
        Rule(LinkExtractor(restrict_xpaths='(//a[@class="lister-page-next next-page"])[1]')),
    )

    def parse_item(self, response):
        yield{
        'title': response.xpath('//div[@class="title_wrapper"]/h1/text()').get(),
        'year': response.xpath('//span[@id="titleYear"]/a/text()').get(),
        'duration': response.xpath('normalize-space((//time)[1]/text())').get(),
        'genre': response.xpath('//div[@class="subtext"]/a[1]/text()').get(),
        'rating': response.xpath('//span[@itemprop="ratingValue"]/text()').get(),
        'movie_url': response.url,
        
        }
