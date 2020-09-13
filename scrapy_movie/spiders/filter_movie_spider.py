#!/usr/bin/env python3
# encoding: utf-8
# @author: hoojo
# @email:    hoojo_@126.com
# @github:   https://github.com/hooj0
# @create date: 2018-09-09
# @copyright by hoojo @2018
# @changelog used scrapy framework spider douban movie data


#===============================================================================
# 标题： used scrapy framework spider douban movie data
#===============================================================================
# 使用：pip install scrapy
#            scrapy crawl search-movie -o douban.csv
#-------------------------------------------------------------------------------
# 描述：利用 scrapy抓取豆瓣电影数据
#-------------------------------------------------------------------------------


#-------------------------------------------------------------------------------
# 通过搜索过滤电影数据
#-------------------------------------------------------------------------------
import scrapy
from scrapy_movie.items import DoubanMovieItem
from bs4 import BeautifulSoup
import time
import logging
import json
import math
from random import random
import sys
import csv
from scrapy.extensions.closespider import CloseSpider

# scrapy crawl filter-movie -o douban-filter.csv
class FilterMovieSpider(scrapy.Spider):
    name = "filter-movie"
    
    def start_requests(self):
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36',
        }
        
        urls = [
            # 5 - 10 分之间的电影
            'https://movie.douban.com/j/new_search_subjects?sort=T&range=5,10&tags=%E7%94%B5%E5%BD%B1&start=0'
        ]
        
        self.headers = headers
        self.movies = set(self.read(filename='movies.csv'))
        
        self.log("Read csv file data count: %s" % len(self.movies))
    
        for url in urls:
            yield scrapy.Request(url=url, headers=headers, callback=self.parse)

    def parse(self, response):

        movies = json.loads(response.body)['data']
        #self.log("movies: %s" % movies)
        if len(movies) == 0:
            raise CloseSpider('finish exit spider.')
        
        for subject in movies:
            #self.log("item id: %s" % subject['id'])
            #self.log("item subject: %s" % subject)
            
            try:
                if subject['id'] in self.movies:
                    continue
                
                item = DoubanMovieItem()
                
                item['ranking'] = subject['id']
                item['score'] = subject['rate']
                item['movie_name'] = subject['title']
                item['link'] = subject['url']
                
                request = scrapy.Request(url=subject['url'], headers=self.headers, callback=self.parse_rating_people)
                request.meta['item'] = item
                
                time.sleep(math.floor(random() * 3))
                yield request
            except:
                self.log("Exception: ===============================================>  %s" % (subject), level=logging.ERROR)
                #print("Unexpected error:", sys.exc_info()[0])
                raise # 未知异常，再次抛出         
                
                
        urls = response.url.split("=")
        urls[-1] = str(int(urls[-1]) + 20)  
        next_url = "=".join(urls)
        if next_url:
            time.sleep(math.floor(random() * 10))
            
            self.log("next_url: %s" % next_url)
            yield scrapy.Request(next_url, headers=self.headers)


    def parse_rating_people(self, response):
        
        item = response.meta['item']
        item['score_num'] = response.xpath('.//a[@class="rating_people"]/span[last()]/text()').extract_first().strip()
        
        if int(item['score_num']) > 60000 and float(item['score']) >= 5.5:
            #print("热门：", item['score_num'])
            #self.log("movie: %s" % item)
            yield item
        else:
            yield None
            
    def read(self, filename = 'target.csv'):
        csv_file = open(filename, 'r', encoding='ANSI')
        data = csv.reader(csv_file)
        
        movies = []
        for line in data:
            if line is not None:
                movies.append(line[2])
                #self.log("=====> %s" % line)
            
        csv_file.close()
        return movies        