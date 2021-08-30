# coding: utf-8
# Name: mycrawler.py
# Date: 20190418
# Version: 1.0

"""Library for crawler with proxy agents.

First get free agents from xicidaili. Then test them, and put the good ones into a queue of proxy agents. The crawler is
programmed with multithreads, with the producer-consumer model. If it is imported as a module, the user can only
constructed the parsement part of the specified crawler. Other works can be left to this module.

classes:
        ProxyAgentProduct
        ProxyTest
        HtmlProducer
        HtmlParser
        WriteToFile
staticmethod:
        ProxyAgentProduct.get_one_page: A function used multiple times, so use the decorator to make it static.
        WriteToFile.write_to_csv:
functions:
        url_queue: Generate url queue for this module.
        create_thread:
        main: Have 2 default parameters: html_parser=HtmlParser, q_url=None. The 1st is a callback of the
        class. When imported as a module, a new child class inherit mycrawler.HtmlParser with rewritten method
        'parse_one_page' can be called by mycrawler.main(). The 2rd parameter is the queue of url of requested page.
public variables:
        user_agents: A set of User-Agent. Almost each time to make a url request, it is invoked.
"""
from typing import List

import csv
import re
import time
import datetime
import random

import requests
from requests.exceptions import RequestException
from bs4 import BeautifulSoup
import threading
import queue

# user_agent列表。断行用\，如果有大中小括号，不需要特别的断行符号。
user_agents = [
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/22.0.1207.1 Safari/537.1",
    "Mozilla/5.0 (X11; CrOS i686 2268.111.0) AppleWebKit/536.11 (KHTML, like Gecko) Chrome/20.0.1132.57 Safari/536.11",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/20.0.1092.0 Safari/536.6",
    "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/20.0.1090.0 Safari/536.6",
    "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/19.77.34.5 Safari/537.1",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/536.5 (KHTML, like Gecko) Chrome/19.0.1084.9 Safari/536.5",
    "Mozilla/5.0 (Windows NT 6.0) AppleWebKit/536.5 (KHTML, like Gecko) Chrome/19.0.1084.36 Safari/536.5",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3",
    "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_0) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1063.0 \
    Safari/536.3",
    "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1062.0 Safari/536.3",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1062.0 Safari/536.3",
    "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3",
    "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3",
    "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.0 Safari/536.3",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/535.24 (KHTML, like Gecko) Chrome/19.0.1055.1 Safari/535.24",
    "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/535.24 (KHTML, like Gecko) Chrome/19.0.1055.1 Safari/535.24"
]


class ProxyAgentProduct(threading.Thread):
    def __init__(self, thread_name, q_agent_raw, num_page=4):
        super().__init__()
        url_base = 'https://www.xicidaili.com/nn/'
        self.urls = [url_base + str(offset + 1) for offset in range(num_page)]
        self.q_agent_raw = q_agent_raw
        self.thread_name = thread_name

    def run(self):
        while True:
            for url in self.urls:
                html = self.get_one_page(url)
                if html:
                    self.parse_one_page(html)
                time.sleep(random.random() + 2.0)
            while True:
                now = datetime.datetime.now()
                if now.hour != 7:
                    time.sleep(60)
                else:
                    break

    @staticmethod
    def get_one_page(url):
        # 利用闭包：
        user_agent = random.choice(user_agents)
        headers = {'User-Agent': user_agent}
        try:
            response = requests.get(url, headers=headers, timeout=2.0)
            if response.status_code == 200:
                response.encoding = 'utf-8'
                return response.text
            else:
                return None
        except RequestException as e:
            print('Exception happened: ',e)

    def parse_one_page(self, html):
        """利用BeautifulSoup方法，解析html文本。
            args:html文本。
            return：代理列表。列表中每个元素为字典，key为ip,port,type,last_check，值为解析后对应的值。
        """
        soup = BeautifulSoup(html, 'lxml')
        trs = soup.find_all(name='table', attrs={'id': 'ip_list'})[0].find_all(name='tr')
        del trs[0]
        for tr in trs:
            proxy_dict = dict()
            proxy_dict['ip'] = tr.find_all(name='td')[1].string
            proxy_dict['port'] = tr.find_all(name='td')[2].string
            proxy_dict['type'] = tr.find_all(name='td')[5].string.lower()
            proxy_dict['last_check'] = tr.find_all(name='td')[8].string
            self.q_agent_raw.put(proxy_dict)


class ProxyTest(threading.Thread):
    # 初始化：
    def __init__(self, thread_name, q_agent_raw, q_agent_tested):
        threading.Thread.__init__(self)
        self.thread_name = thread_name
        self.q_agent_raw = q_agent_raw
        self.q_agent_tested = q_agent_tested

    # 开启多线程的run()函数：
    def run(self):
        while True:
            proxy_agent = self.q_agent_raw.get()
            # 这里可选择httpbin检测连接，或者baidu检测连接：
            # self.test_httpbin(proxy_agent)
            self.test_baidu(proxy_agent)

    # 以专门测试网站httpbin.org，测试连接速度和ip:
    def test_httpbin(self, proxy_agent):
        url = 'https://httpbin.org/ip'
        headers = {'User-Agent': 'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0)'}
        proxies = {proxy_agent['type']: proxy_agent['ip'] + ':' + proxy_agent['port']}
        try:
            time_start = time.time()
            requests.get(url, headers=headers, proxies=proxies, timeout=3)
            time_end = time.time()
            lag = time_end - time_start
            proxy_agent['lag'] = lag
            # 标记检测时间，从而限定代理的生命周期：
            check_time = datetime.datetime.now()
            proxy_agent['check_time'] = check_time
            self.q_agent_tested.put(proxy_agent)
            print('One paoxy agent passes the exam:' + self.thread_name)
        except Exception as e:
            pass

    # 以百度测试连接速度:以下暂时未改写
    def test_baidu(self, proxy_agent):
        url = 'https://www.baidu.com/'
        headers = {'User-Agent': 'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0)'}
        proxies = {proxy_agent['type']: proxy_agent['ip'] + ':' + proxy_agent['port']}
        try:
            time_start = time.time()
            requests.get(url, headers=headers, proxies=proxies, timeout=3)
            time_end = time.time()
            lag = time_end - time_start
            proxy_agent['lag'] = lag
            # 标记检测时间，从而限定代理的生命周期：
            check_time = datetime.datetime.now()
            proxy_agent['check_time'] = check_time
            self.q_agent_tested.put(proxy_agent)
            print('One paoxy agent passes the exam:' + self.thread_name)
        except Exception as e:
            pass


# 构建html生产者线程类，返回的是html数据：
class HtmlProducer(threading.Thread):
    # 注意：为了在作为父类被引用的初始化时，更简洁，需要将参数设置默认值。
    def __init__(self, thread_name=None, user_agents=None, q_agent_tested=None, q_url=None, q_html=None):
        threading.Thread.__init__(self)
        self.thread_name = thread_name
        self.user_agents = user_agents
        self.q_agent_tested = q_agent_tested
        self.q_url = q_url
        # q_html数据类型为dict，包含url和html文档两个key:
        self.q_html = q_html

    def run(self):
        while True:
            if self.q_url.empty():
                break
            else:
                url = self.q_url.get()
                html = self.get_one_page(url)
                if html:
                    html_dict = {'url': url, 'html': html}
                    self.q_html.put(html_dict)
                # 下面这个线程休眠，时长可以调试：
                time.sleep((0.1+ 0.1 * random.random())/(self.q_agent_tested.qsize()+1) )

    def get_one_page(self, url):
        # 随机选择请求头：
        user_agent = random.choice(self.user_agents)
        headers = {'User-Agent': user_agent}
        proxy_agent = self.q_agent_tested.get()
        # proxy_agent可以是空集，即不使用代理：
        if not proxy_agent:
            try:
                response = requests.get(url, headers=headers, timeout=1.0)
                if response.status_code == 200:
                    response.encoding = 'utf-8'
                    self.put_back_or_not(proxy_agent)
                    return response.text
                else:
                    self.put_back_or_not(proxy_agent)
                    return None
            except RequestException:
                # 考虑了代理服务器的稳定性，如果异常，将url在放回url queue：
                self.q_url.put(url)
                self.put_back_or_not(proxy_agent)
        # 非空时使用代理：
        else:
            proxies = {proxy_agent['type']: proxy_agent['ip'] + ':' + proxy_agent['port']}
            try:
                response = requests.get(url, headers=headers, proxies=proxies, timeout=3.0)
                if response.status_code == 200:
                    response.encoding = 'utf-8'
                    self.put_back_or_not(proxy_agent)
                    return response.text
                else:
                    self.put_back_or_not(proxy_agent)
                    return None
            except RequestException:
                self.put_back_or_not(proxy_agent)
                return None

    def put_back_or_not(self, proxy_agent):
        if not proxy_agent:
            self.q_agent_tested.put(proxy_agent)
        elif proxy_agent['check_time'] + datetime.timedelta(hours=24) < datetime.datetime.now():
            self.q_agent_tested.put(proxy_agent)


# 构建html消费者类。对html的queue进行解析，从html代码中提取所需的信息。
class HtmlParser(threading.Thread):
    # 初始化：
    def __init__(self, thread_name, user_agents, q_html, q_result):
        threading.Thread.__init__(self)
        self.thread_name = thread_name
        self.user_agents = user_agents
        self.q_html = q_html
        self.q_result = q_result

    def run(self):
        while True:
            html_dict = self.q_html.get()
            self.parse_one_page(html_dict)

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
                html_view_count = ProxyAgentProduct.get_one_page(url_view_count)
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


# 构建消费者类，提取结果队列的数据，写入csv文件：
class WriteToFile(threading.Thread):
    # 初始化：
    def __init__(self, thread_name, q_result):
        threading.Thread.__init__(self)
        self.thread_name = thread_name
        self.q_result = q_result

    # 写入多线程间不共享数据，以免出错。因此每个线程写一个文件，以线程名命名。
    def run(self):
        data = []
        counter = 0
        while True:
            # 以列表的方式，攒足了列表然后写入，内存消耗非常大：
            data_element = self.q_result.get()
            data.append(data_element)
            counter += 1
            # 下面这种方式写入，相当于设立缓存：
            if len(data) > 100:
                directory = 'd:\\baidu\\' + str(datetime.datetime.now().date()) + self.thread_name + '-' + str(
                    int(counter / 10000)) + '.csv'
                WriteToFile.write_to_csv(directory, data)
                data = []
                print('The {0}th item has been written to file by {1}.'.format(counter, self.thread_name))

    @staticmethod
    def write_to_csv(directory, data):
        '''这里参数data是字典组成的list格式。'''
        with open(directory, 'a', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f)
            # writer.writerow(data[0].keys())
            for item in data:
                writer.writerow(item.values())


# 构建产生url queue的函数：
def url_queue(start_num=1, end_num=1500000):
    q_url = queue.Queue()
    while start_num < end_num:
        url = 'https://baike.baidu.com/view/' + str(start_num) + '.html'
        q_url.put(url)
        start_num += 1
    return q_url


def create_thread(html_parser, q_agent_raw, q_agent_tested, q_url, q_html, q_result, num_tester=10, num_producer=10,
                  num_consumer=16, num_writer=4):
    # 爬取代理服务器只需1个线程即可：
    t_proxy_agent_product = [ProxyAgentProduct('proxy_agent_product', q_agent_raw, num_page=4)]

    t_testers = []
    for i in range(num_tester):
        thread_name_producer = 'thread_tester ' + str(i)
        t_tester = ProxyTest(thread_name_producer, q_agent_raw, q_agent_tested)
        t_testers.append(t_tester)
    t_producers = []
    for i in range(num_producer):
        thread_name_producer = 'thread_producer ' + str(i)
        t_producer = HtmlProducer(thread_name_producer, user_agents, q_agent_tested, q_url, q_html)
        t_producers.append(t_producer)
    # 开启消费者线程，个数自己定，可反复尝试：
    t_consumers = []
    for i in range(num_consumer):
        thread_name_consumer = 'thread_consumer ' + str(i)
        t_consumer = html_parser(thread_name_consumer, user_agents, q_html, q_result)
        t_consumers.append(t_consumer)
    # 开启写入线程，个数自己定，可反复尝试：
    t_writers = []
    for i in range(num_writer):
        thread_name_writer = 'thread_writer ' + str(i)
        t_writer = WriteToFile(thread_name_writer, q_result)
        t_writers.append(t_writer)
    # 全部线程列表：
    t_total = t_proxy_agent_product.copy()
    t_total.extend(t_testers)
    t_total.extend(t_producers)
    t_total.extend(t_consumers)
    t_total.extend(t_writers)
    return t_total


# 主程序，初始化队列，启动多线程：
def main(html_parser=HtmlParser, q_url=None):
    # 定义所需queue：
    q_agent_raw = queue.Queue()
    q_agent_tested = queue.Queue(100)
    #放入None，即不使用代理情况：
    q_agent_tested.put(None)
    q_html = queue.Queue(50)
    q_result = queue.Queue(50)
    if not q_url:
        q_url = url_queue()
    print('Initialization finished.')

    t_total = create_thread(html_parser, q_agent_raw, q_agent_tested, q_url, q_html, q_result)
    # 一次性开启多线程。构造足够好的话，该阻塞阻塞，一块开没问题。
    for t in t_total:
        t.start()


    # # 测试块：
    # while True:
    #     print('Size of q_agent_raw:', q_agent_raw.qsize())
    #     print('Size of q_agent_tested:',q_agent_tested.qsize())
    #     print('Size of q_html:', q_html.qsize())
    #     print('Size of q_url:', q_url.qsize())
    #     print('Size of q_result:', q_result.qsize())
    #     time.sleep(10)


    # 线程锁：
    for t in t_total:
        t.join()

    # 运行完毕，输出提示。我觉得目前状态永远不可能完毕。
    print('The whole job is complete.')


if __name__ == '__main__':
    main()
