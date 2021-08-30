# coding: utf-8
# 本程序利用自写模块mycrawler，爬取百度百科。算是一种尝试：

import json,csv
import re
import time
import datetime
import random

import requests
from requests.exceptions import RequestException
from bs4 import BeautifulSoup
import threading
import queue

import mycrawler


# 继承mycrawler的HtmlParser模块，改写parse_one_page方法。这里有一点不漂亮，继承父类时，子类的初始化参数还都需要写出来。
class BaiduHtmlParser(mycrawler.HtmlParser):
    #初始化：
    def __init__(self):
        super().__init__()

    def parse_one_page(self, html_dict):
        html = html_dict['html']
        url = html_dict['url']
        soup = BeautifulSoup(html, 'lxml')
        if soup.find_all(name='p', attrs={'class': 'sorryCont'}):
            return None
        else:
            paper_dict = {'url': url}
            # 爬取词条名称：
            try:
                names = soup.find_all(name='dd', attrs={'class': 'lemmaWgt-lemmaTitle-title'})[0]
                name = names.h1.string
                paper_dict['name'] = name
            except:
                paper_dict['name'] = None
            # 爬取摘要：
            try:
                abstracts = soup.find_all(name='div', attrs={'class': 'lemma-summary', 'label-module': 'lemmaSummary'})
                abstract = abstracts[0].text.strip()
                paper_dict['abstract'] = abstract
            except:
                paper_dict['abstract'] = None
            # 爬取标签：
            try:
                tags = soup.find_all(name='dd', attrs={'id': 'open-tag-item'})
                tags_list = [tag.strip() for tag in tags[0].text.split('，')]
                paper_dict['tag'] = tags_list
            except:
                paper_dict['tag'] = None
            # 爬取浏览次数。
            try:
                pattern = re.compile('!function.*?newLemmaIdEnc:"(.*?)"', re.S)
                items = re.findall(pattern, html)
                url_view_count = 'https://baike.baidu.com/api/lemmapv?id=' + items[0]
                html_view_count = mycrawler.ProxyAgentProduct.get_one_page(url_view_count)
                pattern_view_count = re.compile('\\{"pv"\\:(.*?)\\}', re.S)
                view_count = re.findall(pattern_view_count, html_view_count)[0]
                paper_dict['view_count'] = view_count
            except:
                paper_dict['view_count'] = None
            # 爬取编辑次数。经验：对大的html文件，正则搜索很慢，所以最好先采用beautifulsoup解析，然后正则提取。
            try:
                edit_count_block = soup.find_all(name='dd', attrs={'class': 'description'})[0].text
                pattern_edit_count = re.compile('编辑次数：(\d+)次', re.S)
                edit_count = re.findall(pattern_edit_count, edit_count_block)[0]
                paper_dict['edit_count'] = edit_count
            except:
                paper_dict['edit_count'] = None
            self.q_result.put(paper_dict)


mycrawler.main(html_parser=BaiduHtmlParser)
