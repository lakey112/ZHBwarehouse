#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import codecs
import csv
import json
import os
import random
import sys
import traceback
from collections import OrderedDict
from datetime import date, datetime, timedelta
from time import sleep
import requests
from lxml import etree
from tqdm import tqdm
import pandas as pd


class Weibo(object):
    def __init__(self, config):
        """Weibo类初始化"""
        self.validate_config(config)
        self.filter = 0  # 取值范围为0、1,程序默认值为0,代表要爬取用户的全部微博,1代表只爬取用户的原创微博
        since_date = str(config['since_date'])
        if since_date.isdigit():
            since_date = str(date.today() - timedelta(int(since_date)))
        self.since_date = '2019-12-08'  # 起始时间，即爬取发布日期从该值到现在的微博，形式为yyyy-mm-dd
        self.write_mode = ['csv']  # 结果信息保存类型，为list形式，可包含txt、csv、json、mongo和mysql五种类型
        self.pic_download = 0  # 取值范围为0、1,程序默认值为0,代表不下载微博原始图片,1代表下载
        self.video_download = 0  # 取值范围为0、1,程序默认为0,代表不下载微博视频,1代表下载
        self.cookie = {'Cookie': 'SINAGLOBAL=1107222056883.69.1576903265256; _s_tentry=www.baidu.com; Apache=8727965144699.057.1579616113642; ULV=1579616113647:2:1:1:8727965144699.057.1579616113642:1576903265279; SSOLoginState=1579719294; login_sid_t=ebca49178c1a59bc2a71dac319ddba10; cross_origin_proto=SSL; un=15574477731; UOR=finance.eastmoney.com,widget.weibo.com,login.sina.com.cn; s_cc=true; s_sq=%5B%5BB%5D%5D; SUBP=0033WrSXqPxfM725Ws9jqgMF55529P9D9WWr-F5Xfs5sRq6JS2Wqh0R05JpX5KMhUgL.FoM0S0zNeoq7So52dJLoI0qLxKBLB.BL1K.LxK.L1-qLBoeLxKqL1-eLB-2LxKqL1K-LBKeLxKqL1h-L1K-LxKML1h2LBo-t; ALF=1612493839; SCF=Auj6uVZv7rlIgN15ze2zWMpNq0xGl0kAgmQMAUJ_H0xC5VQUjEi5z9MuDhqsT6qInh9UjhV8nwYurLS9uyK3Daw.; SUB=_2A25zP_DbDeRhGeFN7FAW8ijMzTyIHXVQTWUTrDV8PUNbmtAfLXbikW9NQ_QvSyaDXTorysy7XCwr19vij7ERRCQO; SUHB=00xZvVE-fqEnBJ'}
        self.mysql_config = config.get('mysql_config')  # MySQL数据库连接配置，可以不填
        user_id_list = config['user_id_list']
        if not isinstance(user_id_list, list):
            if not os.path.isabs(user_id_list):
                user_id_list = os.path.split(
                    os.path.realpath(__file__))[0] + os.sep + user_id_list
            self.user_config_file_path = user_id_list  # 用户配置文件路径
            user_config_list = self.get_user_config_list(user_id_list)
        else:
            self.user_config_file_path = ''
            user_config_list = [{
                'user_id': user_id,
                'since_date': self.since_date
            } for user_id in user_id_list]
        self.user_config_list = user_config_list  # 要爬取的微博用户的user_config列表
        self.user_config = {}  # 用户配置,包含用户id和since_date
        self.start_time = ''  # 获取用户第一条微博时的时间
        self.user = {}  # 存储爬取到的用户信息
        self.got_num = 0  # 存储爬取到的微博数
        self.weibo = []  # 存储爬取到的所有微博信息
        self.weibo_id_list = []  # 存储爬取到的所有微博id

    def validate_config(self, config):
        """验证配置是否正确"""

        # 验证filter、pic_download、video_download
        argument_lsit = ['filter', 'pic_download', 'video_download']
        for argument in argument_lsit:
            if config[argument] != 0 and config[argument] != 1:
                sys.exit(u'%s值应为0或1,请重新输入' % config[argument])

        # 验证since_date
        since_date = str(config['since_date'])
        if (not self.is_date(since_date)) and (not since_date.isdigit()):
            sys.exit(u'since_date值应为yyyy-mm-dd形式或整数,请重新输入')

        # 验证write_mode
        write_mode = ['txt', 'csv', 'json', 'mongo', 'mysql']
        if not isinstance(config['write_mode'], list):
            sys.exit(u'write_mode值应为list类型')
        for mode in config['write_mode']:
            if mode not in write_mode:
                sys.exit(
                    u'%s为无效模式，请从txt、csv、json、mongo和mysql中挑选一个或多个作为write_mode' %
                    mode)

        # 验证user_id_list
        user_id_list = config['user_id_list']
        if (not isinstance(user_id_list,
                           list)) and (not user_id_list.endswith('.txt')):
            sys.exit(u'user_id_list值应为list类型或txt文件路径')
        if not isinstance(user_id_list, list):
            if not os.path.isabs(user_id_list):
                user_id_list = os.path.split(
                    os.path.realpath(__file__))[0] + os.sep + user_id_list
            if not os.path.isfile(user_id_list):
                sys.exit(u'不存在%s文件' % user_id_list)

    def is_date(self, since_date):
        """判断日期格式是否正确"""
        try:
            if ':' in since_date:
                datetime.strptime(since_date, '%Y-%m-%d %H:%M')
            else:
                datetime.strptime(since_date, '%Y-%m-%d')
            return True
        except ValueError:
            return False

    def str_to_time(self, text):
        """将字符串转换成时间类型"""
        result = datetime.strptime(text, '%Y-%m-%d')
        return result

    def handle_html(self, url):
        """处理html"""
        try:
            html = requests.get(url, cookies=self.cookie).content
            selector = etree.HTML(html)
            return selector
        except Exception as e:
            print('Error: ', e)
            traceback.print_exc()


    def print_user_info(self):
        """打印微博用户信息"""
        print(u'用户昵称: %s' % self.weibo['nickname'])
        print(u'微博数: %d' % self.weibo['weibo_num'])
        print(u'关注数: %d' % self.weibo['following'])
        print(u'粉丝数: %d' % self.weibo['followers'])


    def get_user_info(self, selector):
        """获取用户昵称、微博数、关注数、粉丝数"""
        try:

            user_info = selector.xpath("//div[@class='tip2']/*/text()")

            weibo_num = int(user_info[0][3:-1])
            following = int(user_info[1][3:-1])
            followers = int(user_info[2][3:-1])

            self.weibo['weibo_num'] = weibo_num
            self.weibo['following'] = following
            self.weibo['followers'] = followers
            self.print_user_info()

            print('*' * 100)
        except Exception as e:
            print('Error: ', e)
            traceback.print_exc()




    def get_original_weibo(self, info, weibo_id):
        """获取原创微博"""
        try:

            weibo_content = info.xpath("div[@class = 'card']/div[@class = 'card-feed']/div[@class = 'content']/p[@class = 'txt']")
            wb_content = ""
            if len(weibo_content) > 1:

                weibo_content = info.xpath("div[@class = 'card']/div[@class = 'card-feed']/div[@class = 'content']/p[@node-type = 'feed_list_content_full']//text()")

                for text in weibo_content:
                    text.strip(" ").replace("\n","").replace(" ","")
                    if text:
                        wb_content = wb_content + text

                weibo_content = wb_content
            else:
                weibo_content = info.xpath("div[@class = 'card']/div[@class = 'card-feed']/div[@class = 'content']/p[@node-type = 'feed_list_content']//text()")

                for text in weibo_content:
                    text.strip(" ").replace("\n", "").replace(" ", "")
                    if text:
                        wb_content = wb_content + text
                weibo_content = wb_content

            return weibo_content
        except Exception as e:
            print('Error: ', e)
            traceback.print_exc()

    def get_retweet(self, info, weibo_id):
        """获取转发微博"""
        try:
            retweet = info.xpath("div[@class = 'card']/div[@class = 'card-feed']/div[@class = 'content']/p[@class = 'txt']")
            retweet_reason=""
            if len(retweet) > 1:
                retweet = info.xpath("div[@class = 'card']/div[@class = 'card-feed']/div[@class = 'content']/p[@node-type = 'feed_list_content_full']//text()")
                for r in retweet:
                    r.strip(" ").replace("\n","").replace(" ","")
                    retweet_reason = retweet_reason + r
            else:
                retweet = info.xpath(
                    "div[@class = 'card']/div[@class = 'card-feed']/div[@class = 'content']/p[@node-type = 'feed_list_content']//text()")
                for r in retweet:
                    r.strip(" ").replace("\n", "").replace(" ", "")
                    retweet_reason = retweet_reason + r

            rt_content = ""
            rt_con = info.xpath("div[@class = 'card']/div[@class = 'card-feed']/div[@class = 'content']/div[@class='card-comment']/div[@class = 'con']/div/div//p[@class = 'txt']")
            if len(rt_con) > 1:
                rt_con = info.xpath("div[@class = 'card']/div[@class = 'card-feed']/div[@class = 'content']/div[@class='card-comment']/div[@class = 'con']/div/div//p[@node-type = 'feed_list_content_full']//text()")
                for rt in rt_con:
                    rt.strip(" ").replace("\n","").replace(" ","")
                    rt_content = rt_content + rt
            else:
                rt_con = info.xpath(
                    "div[@class = 'card']/div[@class = 'card-feed']/div[@class = 'content']/div[@class='card-comment']/div[@class = 'con']/div/div//p[@node-type = 'feed_list_content']//text()")
                for rt in rt_con:
                    rt.strip(" ").replace("\n", "").replace(" ", "")
                    rt_content = rt_content + rt
            original_user = info.xpath("//div[@node-type='feed_list_forwardContent']/a/text()")[0]

            wb_content = (retweet_reason + ' ' + u'原始用户: ' +original_user + ' ' + u'转发内容: ' + rt_content)
            return wb_content
        except Exception as e:
            print('Error: ', e)
            traceback.print_exc()

    def is_original(self, info):
        """判断微博是否为原创微博"""
        is_original = info.xpath("div[@class = 'card']/div[@class = 'card-feed']/div[@class = 'content']/div[@class = 'card-comment']")

        if len(is_original) > 0:
            return False
        else:
            return True

    def get_weibo_content(self, info, is_original):
        """获取微博内容"""
        try:
            weibo_id = info.xpath('@mid')[0]

            if is_original:
                weibo_content = self.get_original_weibo(info, weibo_id)
            else:
                weibo_content = self.get_retweet(info, weibo_id)
            return weibo_content
        except Exception as e:
            print('Error: ', e)
            traceback.print_exc()

    def get_publish_time(self, info):
        """获取微博发布时间"""
        try:
            str_time = info.xpath("div[@class = 'card']/div[@class = 'card-feed']/div[@class = 'content']/p[@class='from']/a")

            publish_time = str_time[0].xpath("text()")[0].replace("\n", "").strip(" ")

            if u'刚刚' in publish_time:
                publish_time = datetime.now().strftime('%Y-%m-%d %H:%M')
            elif u'分钟' in publish_time:
                minute = publish_time[:publish_time.find(u'分钟')]
                minute = timedelta(minutes=int(minute))
                publish_time = (datetime.now() -
                                minute).strftime('%Y-%m-%d %H:%M')
            elif u'今天' in publish_time:
                today = datetime.now().strftime('%Y-%m-%d')
                time = publish_time[3:]
                publish_time = today + ' ' + time
                if len(publish_time) > 16:
                    publish_time = publish_time[:16]
            # elif u'月' in publish_time:
            #     year = datetime.now().strftime('%Y')
            #     month = publish_time[0:2]
            #     day = publish_time[3:5]
            #     time = publish_time[7:12]
            #     publish_time = year + '-' + month + '-' + day + ' ' + time
            else:
                publish_time = publish_time[:17]
            return publish_time
        except Exception as e:
            print('Error: ', e)
            traceback.print_exc()


    def get_weibo_footer(self, info):
        """获取微博点赞数、转发数、评论数"""
        try:
            footer = {}
            pattern = r'\d+'
            str_footer = info.xpath("div[@class='card']/div[@class='card-act']/ul//li")


            rn = str_footer[1].xpath("a/text()")[0]
            retweet_num = rn.replace("转发" , "").replace(' ','')

            if retweet_num is '':
                footer['retweet_num'] = 0
            else:
                footer['retweet_num'] = int(retweet_num)

            cn = str_footer[2].xpath("a/text()")[0]
            com_num = cn.replace("评论 ", "").replace(' ','')
            if com_num is '':
                footer['com_num'] = 0
            else:
                footer['com_num'] = int(com_num)

            up_num = str_footer[3].xpath("a/em/text()")
            if len(up_num) is 0 :
                footer['up_num'] = 0
            else:
                footer['up_num'] = int(up_num[0])
            return footer
        except Exception as e:
            print('Error: ', e)
            traceback.print_exc()

    def get_one_weibo(self, info):
        """获取一条微博的全部信息"""
        try:

            nickname = info.xpath("div[@class = 'card']/div[@class = 'card-feed']/div[@class = 'content']/div[@class = 'info']/div/a[@class = 'name']")
            self.user['nickname'] = nickname[0].xpath("text()")[0]

            weibo = OrderedDict()
            is_original = self.is_original(info)

            weibo['id'] = info.xpath('@mid')[0]
            weibo['nickname'] = self.user['nickname']
            weibo['content'] = self.get_weibo_content(info,
                                                      is_original)  # 微博内容

            if not self.filter:
                weibo['original'] = is_original  # 是否原创微博

            weibo['publish_time'] = self.get_publish_time(info)  # 微博发布时间
            footer = self.get_weibo_footer(info)
            weibo['up_num'] = footer['up_num']  # 微博点赞数
            weibo['retweet_num'] = footer['retweet_num']  # 转发数
            weibo['comment_num'] = footer['com_num']  # 评论数

            return weibo
        except Exception as e:
            print('Error: ', e)
            traceback.print_exc()

    def print_one_weibo(self, weibo):
        """打印一条微博"""
        print(u'昵称：%s' % weibo['nickname'])
        print(weibo['content'])
        print(u'发布时间：%s' % weibo['publish_time'])
        print(u'点赞数：%d' % weibo['up_num'])
        print(u'转发数：%d' % weibo['retweet_num'])
        print(u'评论数：%d' % weibo['comment_num'])
        print(u'是否原创：%s' % weibo['original'])
        print(u'count：%d' % self.got_num)

    def get_one_page(self, page, url):
        """获取第page页的全部微博"""
        try:
            url = url + '&page=%d' % (page)
            selector = self.handle_html(url)
            info = selector.xpath("//div[@class='card-wrap']")
            if 1:
                for i in range(0, len(info)-3):
                    wb = self.get_one_weibo(info[i])
                    if wb:
                        if wb['id'] in self.weibo_id_list:
                            continue
                        self.print_one_weibo(wb)
                        self.weibo.append(wb)
                        self.weibo_id_list.append(wb['id'])
                        self.got_num += 1
                        print('-' * 100)
        except Exception as e:
            print('Error: ', e)
            traceback.print_exc()

    def get_filepath(self, type, date):
        """获取结果文件路径"""
        try:
            file_dir = os.path.split(
                os.path.realpath(__file__)
            )[0] + os.sep + 'weibo' + os.sep + 'wuhan'
            print('file_dir', file_dir)
            file_path = file_dir + os.sep + '12-' + str(date) +  '.' + type

            return file_path
        except Exception as e:
            print('Error: ', e)
            traceback.print_exc()

    def write_csv(self, wrote_num,date):
        """将爬取的信息写入csv文件"""
        try:
            result_headers = [
                '微博id',
                '微博昵称',
                '微博正文',
                '是否原创',
                '发布时间',
                '点赞数',
                '转发数',
                '评论数',
            ]


            result_data = [w.values() for w in self.weibo[wrote_num:]]

            if sys.version < '3':  # python2.x
                reload(sys)
                sys.setdefaultencoding('utf-8')
                with open(self.get_filepath('csv'), 'ab') as f:
                    f.write(codecs.BOM_UTF8)
                    writer = csv.writer(f)
                    if wrote_num == 0:
                        writer.writerows([result_headers])
                    writer.writerows(result_data)
            else:  # python3.x
                with open(self.get_filepath('csv',date), 'a', encoding='utf-8-sig', newline='') as f:

                    writer = csv.writer(f)

                    if wrote_num == 0:
                        writer.writerows([result_headers])

                    writer.writerows(result_data)
            print(u'%d条微博写入csv文件完毕,保存路径:' % self.got_num)
            print(self.get_filepath('csv'))
        except Exception as e:
            print('Error: ', e)
            traceback.print_exc()

    def get_weibo_info(self,date,keyword):
        """获取微博信息"""
        try:
            date = str(date)[:10]
            for hour in range(0, 24):
                hour_end = hour + 1
                url = 'https://s.weibo.com/weibo?q=%s&typeall=1&suball=1&timescope=custom:%s-%d:%s-%d&Refer=g' % (keyword,date, hour, date,hour_end)

                print('url: ', url)
                selector = self.handle_html(url)


                page_num = selector.xpath("//ul[@class = 's-scroll']/li")
                if len(page_num) is 0:
                    page_num = 1
                else:
                    page_num = int(page_num[-1].xpath("a/text()")[0].replace(u"第", '').replace(u"页", ''))# 获取微博总页数

                wrote_num = 0
                page1 = 0
                random_pages = random.randint(1, 5)
                for page in tqdm(range(1, page_num + 1), desc='Progress'):

                    self.get_one_page(page, url)  # 获取第page页的全部微博

                    if page - page1 == random_pages and page < page_num:
                        sleep(random.randint(6, 10))
                        page1 = page
                        random_pages = random.randint(1, 5)

            print('添加{}成功'.format(url))

            self.write_csv(wrote_num, date)

            print(u'共爬取' + str(self.got_num) + u'条微博')

        except Exception as e:
            print('Error: ', e)
            traceback.print_exc()

    def get_user_config_list(self, file_name):
        """获取文件中的微博id信息"""
        with open(file_name, 'rb') as f:
            lines = f.read().splitlines()
            lines = [line.decode('utf-8') for line in lines]
            user_config_list = []
            for line in lines:
                info = line.split(' ')
                if len(info) > 0 and info[0].isdigit():
                    user_config = {}
                    user_config['user_id'] = info[0]
                    if len(info) > 2 and self.is_date(info[2]):
                        if len(info) > 3 and self.is_date(info[2] + ' ' +
                                                          info[3]):
                            user_config['since_date'] = info[2] + ' ' + info[3]
                        else:
                            user_config['since_date'] = info[2]
                    else:
                        user_config['since_date'] = self.since_date
                    user_config_list.append(user_config)
        return user_config_list

    def initialize_info(self, user_config):
        """初始化爬虫信息"""
        self.got_num = 0
        self.weibo = []
        self.user = {}
        self.user_config = user_config
        self.weibo_id_list = []

    def start(self,date,keyword):
        """运行爬虫"""
        try:

            print('*' * 100)
            self.get_weibo_info(date,keyword)
            print(u'信息抓取完毕')
            print('*' * 100)

        except Exception as e:
            print('Error: ', e)
            traceback.print_exc()


def main(date,keyword):
    try:
        config_path = os.path.split(
            os.path.realpath(__file__))[0] + os.sep + 'config.json'
        if not os.path.isfile(config_path):
            sys.exit(u'当前路径：%s 不存在配置文件config.json' %
                     (os.path.split(os.path.realpath(__file__))[0] + os.sep))
        with open(config_path) as f:
            config = json.loads(f.read())
        wb = Weibo(config)

        wb.start(date,keyword)  # 爬取微博信息
    except ValueError:
        print(u'config.json 格式不正确，请参考 '
              u'https://github.com/dataabc/weiboSpider#3程序设置')
    except Exception as e:
        print('Error: ', e)
        traceback.print_exc()


if __name__ == '__main__':
    datelist = pd.date_range('12/1/2019','1/31/2020') #mon-day-year
    print(datelist)
    keyword = u"武汉肺炎"
    for date in datelist:
        main(date,keyword)
