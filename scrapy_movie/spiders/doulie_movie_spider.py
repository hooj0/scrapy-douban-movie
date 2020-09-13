#!/usr/bin/env python3
# encoding: utf-8
# @author: hoojo
# @email: hoojo_@126.com
# @github: https://github.com/hooj0
# @create date: 2018-09-09
# @copyright by hoojo @2018
# @changelog used scrapy framework spider douban movie data


#===============================================================================
# 标题： used scrapy framework spider douban movie data
#===============================================================================
# 使用：pip install scrapy
#            scrapy crawl dis-movie -o douban.csv
#-------------------------------------------------------------------------------
# 描述：利用 scrapy抓取豆瓣电影数据
#-------------------------------------------------------------------------------


#-------------------------------------------------------------------------------
# 抓取豆瓣电影 豆列 数据
#-------------------------------------------------------------------------------
import scrapy
from scrapy_movie.items import DoubanMovieItem
from bs4 import BeautifulSoup
import time
import logging
import math
from random import random

# scrapy crawl movie -o douban.csv
class MovieSpider(scrapy.Spider):
    name = "movie"
    
    def start_requests(self):
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36',
        }
        
        urls = [
            'http://www.douban.com/doulist/2443408/',
        ]
        
        self.headers = headers;
        
        for url in urls:
            yield scrapy.Request(url=url, headers=headers, callback=self.parse)

    def parse(self, response):
        #soup = BeautifulSoup(response.body, "html.parser")
        movies = response.xpath('//div[@class="doulist-item"]')
        
        for movie in movies:
            #id = movie.xpath('@id').extract_first().strip()
            #self.log("item id: %s" % id)

            subject = movie.xpath('.//div[@class="bd doulist-subject"]')
            
            try:
                item = DoubanMovieItem()
                
                item['ranking'] = movie.xpath('.//span[@class="pos"]/text()').extract_first().strip()
                item['score'] = subject.xpath('.//div[@class="rating"]/span[@class="rating_nums"]/text()').extract_first().strip()
                item['movie_name'] = subject.xpath('.//div[@class="title"]/a/text()[last()]').extract_first().strip()
                item['score_num'] = subject.xpath('.//div[@class="rating"]/span[last()]/text()').re(r'(\d+)人评价')[0]
                item['link'] = subject.xpath('.//div[@class="post"]/a/@href').extract_first().strip()
                
                if int(item['score_num']) > 80000:
                    #print("热门：", item['score_num'])
                    #self.log("movie: %s" % item)
                    yield item
                else:
                    yield None
                
            except:
                self.log("Exception: ===============================================>  %s" % (movie), level=logging.ERROR)
                #yield scrapy.Request(response.url, headers=self.headers)
                
                
        next_url = response.xpath('//span[@class="next"]/a/@href').extract()
        if next_url:
            time.sleep(math.floor(random() * 30))
            
            self.log("next_url: %s" % next_url[0])
            yield scrapy.Request(next_url[0], headers=self.headers)
