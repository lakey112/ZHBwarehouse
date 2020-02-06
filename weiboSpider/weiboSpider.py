#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import codecs
import copy
import csv
import json
import os
import random
import re
import sys
import traceback
from collections import OrderedDict
from datetime import date, datetime, timedelta

from time import sleep

import requests
from lxml import etree
from requests.adapters import HTTPAdapter
from tqdm import tqdm


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
        if ':' in text:
            result = datetime.strptime(text, '%Y-%m-%d %H:%M')
        else:
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
        # print(u'用户id: %s' % self.user['id'])
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
            # print('***********', up_num)
            if len(up_num) is 0 :
                footer['up_num'] = 0
            else:
                footer['up_num'] = int(up_num[0])
            return footer
        except Exception as e:
            print('Error: ', e)
            traceback.print_exc()

    def extract_picture_urls(self, info, weibo_id):
        """提取微博原始图片url"""
        try:
            a_list = info.xpath('div/a/@href')
            first_pic = 'https://weibo.cn/mblog/pic/' + weibo_id + '?rl=0'
            all_pic = 'https://weibo.cn/mblog/picAll/' + weibo_id + '?rl=1'
            if first_pic in a_list:
                if all_pic in a_list:
                    selector = self.handle_html(all_pic)
                    preview_picture_list = selector.xpath('//img/@src')
                    picture_list = [
                        p.replace('/thumb180/', '/large/')
                        for p in preview_picture_list
                    ]
                    picture_urls = ','.join(picture_list)
                else:
                    if info.xpath('.//img/@src'):
                        preview_picture = info.xpath('.//img/@src')[-1]
                        picture_urls = preview_picture.replace(
                            '/wap180/', '/large/')
                    else:
                        sys.exit(
                            u"爬虫微博可能被设置成了'不显示图片'，请前往"
                            u"'https://weibo.cn/account/customize/pic'，修改为'显示'"
                        )
            else:
                picture_urls = u'无'
            return picture_urls
        except Exception as e:
            return u'无'
            print('Error: ', e)
            traceback.print_exc()

    def get_picture_urls(self, info, is_original):
        """获取微博原始图片url"""
        try:
            weibo_id = info.xpath('@id')[0][2:]
            picture_urls = {}
            if is_original:
                original_pictures = self.extract_picture_urls(info, weibo_id)
                picture_urls['original_pictures'] = original_pictures
                if not self.filter:
                    picture_urls['retweet_pictures'] = u'无'
            else:
                retweet_url = info.xpath("div/a[@class='cc']/@href")[0]
                retweet_id = retweet_url.split('/')[-1].split('?')[0]
                retweet_pictures = self.extract_picture_urls(info, retweet_id)
                picture_urls['retweet_pictures'] = retweet_pictures
                a_list = info.xpath('div[last()]/a/@href')
                original_picture = u'无'
                for a in a_list:
                    if a.endswith(('.gif', '.jpeg', '.jpg', '.png')):
                        original_picture = a
                        break
                picture_urls['original_pictures'] = original_picture
            return picture_urls
        except Exception as e:
            print('Error: ', e)
            traceback.print_exc()

    def get_video_url(self, info, is_original):
        """获取微博视频url"""
        try:
            if is_original:
                div_first = info.xpath('div')[0]
                a_list = div_first.xpath('.//a')
                video_link = u'无'
                for a in a_list:
                    if 'm.weibo.cn/s/video/show?object_id=' in a.xpath(
                            '@href')[0]:
                        video_link = a.xpath('@href')[0]
                        break
                if video_link != u'无':
                    video_link = video_link.replace(
                        'm.weibo.cn/s/video/show', 'm.weibo.cn/s/video/object')
                    wb_info = requests.get(video_link,
                                           cookies=self.cookie).json()
                    video_url = wb_info['data']['object']['stream'].get(
                        'hd_url')
                    if not video_url:
                        video_url = wb_info['data']['object']['stream']['url']
                        if not video_url:  # 说明该视频为直播
                            video_url = u'无'
            else:
                video_url = u'无'
            return video_url
        except Exception as e:
            return u'无'
            print('Error: ', e)
            traceback.print_exc()

    def download_one_file(self, url, file_path, type, weibo_id):
        """下载单个文件(图片/视频)"""
        try:
            if not os.path.isfile(file_path):
                s = requests.Session()
                s.mount(url, HTTPAdapter(max_retries=5))
                downloaded = s.get(url, timeout=(5, 10))
                with open(file_path, 'wb') as f:
                    f.write(downloaded.content)
        except Exception as e:
            error_file = self.get_filepath(
                type) + os.sep + 'not_downloaded.txt'
            with open(error_file, 'ab') as f:
                url = weibo_id + ':' + url + '\n'
                f.write(url.encode(sys.stdout.encoding))
            print('Error: ', e)
            traceback.print_exc()

    def handle_download(self, file_type, file_dir, urls, w):
        """处理下载相关操作"""
        file_prefix = w['publish_time'][:11].replace('-', '') + '_' + w['id']
        if file_type == 'img':
            if ',' in urls:
                url_list = urls.split(',')
                for i, url in enumerate(url_list):
                    file_suffix = url[url.rfind('.'):]
                    file_name = file_prefix + '_' + str(i + 1) + file_suffix
                    file_path = file_dir + os.sep + file_name
                    self.download_one_file(url, file_path, file_type, w['id'])
            else:
                file_suffix = urls[urls.rfind('.'):]
                file_name = file_prefix + file_suffix
                file_path = file_dir + os.sep + file_name
                self.download_one_file(urls, file_path, file_type, w['id'])
        else:
            file_suffix = '.mp4'
            file_name = file_prefix + file_suffix
            file_path = file_dir + os.sep + file_name
            self.download_one_file(urls, file_path, file_type, w['id'])

    def download_files(self, file_type):
        """下载文件(图片/视频)"""
        try:
            if file_type == 'img':
                describe = u'图片'
                key = 'original_pictures'
            else:
                describe = u'视频'
                key = 'video_url'
            print(u'即将进行%s下载' % describe)
            file_dir = self.get_filepath(file_type)
            for w in tqdm(self.weibo, desc='Download progress'):
                if w[key] != u'无':
                    self.handle_download(file_type, file_dir, w[key], w)
            print(u'%s下载完毕,保存路径:' % describe)
            print(file_dir)
        except Exception as e:
            print('Error: ', e)
            traceback.print_exc()

    def get_one_weibo(self, info):
        """获取一条微博的全部信息"""
        try:

            nickname = info.xpath("div[@class = 'card']/div[@class = 'card-feed']/div[@class = 'content']/div[@class = 'info']/div/a[@class = 'name']")
            # print("~~~~~~~~~~~~~~~~nickname:  ", nickname[0].xpath("text()"))
            self.user['nickname'] = nickname[0].xpath("text()")[0]
            # print("~~~~~~~~~~~~~~~~nickname:  ",self.user['nickname'])
            user_url = "https:" + info.xpath("div[@class = 'card']/div[@class = 'card-feed']/div[@class = 'avator']/a/attribute::href")[0].strip("_")
            # print("href:", user_url)
            # user_url = user_url.replace("com", "cn")[:user_url.rfind("?") - 1]
            #
            # selector = self.handle_html(user_url)
            # self.get_user_info(selector)

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

    def is_pinned_weibo(self, info):
        """判断微博是否为置顶微博"""
        kt = info.xpath(".//span[@class='kt']/text()")
        if kt and kt[0] == u'置顶':
            return True
        else:
            return False

    def get_one_page(self, page, url):
        """获取第page页的全部微博"""
        try:
            url = url + '&page=%d' % (page)
            # url = 'https://weibo.cn/search/mblog?hideSearchFrame=&keyword={武汉 肺炎}&advancedfilter=1&starttime={}&endtime={}sort=time&page=%d' % (page)
            # url = url.format(keyword,date_start.strftime("%Y%m%d"), date_end.strftime("%Y%m%d"))
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

    def write_log(self):
        """当程序因cookie过期停止运行时，将相关信息写入log.txt"""
        file_dir = os.path.split(
            os.path.realpath(__file__))[0] + os.sep + 'weibo' + os.sep
        if not os.path.isdir(file_dir):
            os.makedirs(file_dir)
        file_path = file_dir + 'log.txt'
        content = u'cookie已过期，从%s到今天的微博获取失败，请重新设置cookie\n' % self.since_date
        with open(file_path, 'ab') as f:
            f.write(content.encode(sys.stdout.encoding))

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

    def write_txt(self, wrote_num):
        """将爬取的信息写入txt文件"""
        try:
            temp_result = []
            if wrote_num == 0:

                result_header = u'\n\n微博内容: \n'
                result_header = (u'用户信息\n用户昵称：' + self.user['nickname'] +
                                 u'\n用户id: ' +
                                 str(self.user_config['user_id']) +
                                 u'\n微博数: ' + str(self.user['weibo_num']) +
                                 u'\n关注数: ' + str(self.user['following']) +
                                 u'\n粉丝数: ' + str(self.user['followers']) +
                                 result_header)
                temp_result.append(result_header)
            for i, w in enumerate(self.weibo[wrote_num:]):
                temp_result.append(
                    str(wrote_num + i + 1) + ':' + w['content'] + '\n' +
                    u'微博位置: ' + w['publish_place'] + '\n' + u'发布时间: ' +
                    w['publish_time'] + '\n' + u'点赞数: ' + str(w['up_num']) +
                    u'   转发数: ' + str(w['retweet_num']) + u'   评论数: ' +
                    str(w['comment_num']) + '\n' + u'发布工具: ' +
                    w['publish_tool'] + '\n\n')
            result = ''.join(temp_result)
            with open(self.get_filepath('txt'), 'ab') as f:
                f.write(result.encode(sys.stdout.encoding))
            print(u'%d条微博写入txt文件完毕,保存路径:' % self.got_num)
            print(self.get_filepath('txt'))
        except Exception as e:
            print('Error: ', e)
            traceback.print_exc()

    def update_json_data(self, data, weibo_info):
        """更新要写入json结果文件中的数据，已经存在于json中的信息更新为最新值，不存在的信息添加到data中"""
        data['user'] = self.user
        if data.get('weibo'):
            is_new = 1  # 待写入微博是否全部为新微博，即待写入微博与json中的数据不重复
            for old in data['weibo']:
                if weibo_info[-1]['id'] == old['id']:
                    is_new = 0
                    break
            if is_new == 0:
                for new in weibo_info:
                    flag = 1
                    for i, old in enumerate(data['weibo']):
                        if new['id'] == old['id']:
                            data['weibo'][i] = new
                            flag = 0
                            break
                    if flag:
                        data['weibo'].append(new)
            else:
                data['weibo'] += weibo_info
        else:
            data['weibo'] = weibo_info
        return data

    def write_json(self, wrote_num):
        """将爬到的信息写入json文件"""
        data = {}
        path = self.get_filepath('json')
        if os.path.isfile(path):
            with codecs.open(path, 'r', encoding="utf-8") as f:
                data = json.load(f)
        weibo_info = self.weibo[wrote_num:]
        data = self.update_json_data(data, weibo_info)
        with codecs.open(path, 'w', encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
        print(u'%d条微博写入json文件完毕,保存路径:' % self.got_num)
        print(path)

    def info_to_mongodb(self, collection, info_list):
        """将爬取的信息写入MongoDB数据库"""
        try:
            import pymongo
        except ImportError:
            sys.exit(u'系统中可能没有安装pymongo库，请先运行 pip install pymongo ，再运行程序')
        try:
            from pymongo import MongoClient
            client = MongoClient()
            db = client['weibo']
            collection = db[collection]
            if len(self.write_mode) > 1:
                new_info_list = copy.deepcopy(info_list)
            else:
                new_info_list = info_list
            for info in new_info_list:
                if not collection.find_one({'id': info['id']}):
                    collection.insert_one(info)
                else:
                    collection.update_one({'id': info['id']}, {'$set': info})
        except pymongo.errors.ServerSelectionTimeoutError:
            sys.exit(u'系统中可能没有安装或启动MongoDB数据库，请先根据系统环境安装或启动MongoDB，再运行程序')

    def weibo_to_mongodb(self, wrote_num):
        """将爬取的微博信息写入MongoDB数据库"""
        weibo_list = []
        for w in self.weibo[wrote_num:]:
            w['user_id'] = self.user_config['user_id']
            weibo_list.append(w)
        self.info_to_mongodb('weibo', weibo_list)
        print(u'%d条微博写入MongoDB数据库完毕' % self.got_num)

    def mysql_create(self, connection, sql):
        """创建MySQL数据库或表"""
        try:
            with connection.cursor() as cursor:
                cursor.execute(sql)
        finally:
            connection.close()

    def mysql_create_database(self, mysql_config, sql):
        """创建MySQL数据库"""
        try:
            import pymysql
        except ImportError:
            sys.exit(u'系统中可能没有安装pymysql库，请先运行 pip install pymysql ，再运行程序')
        try:
            if self.mysql_config:
                mysql_config = self.mysql_config
            connection = pymysql.connect(**mysql_config)
            self.mysql_create(connection, sql)
        except pymysql.OperationalError:
            sys.exit(u'系统中可能没有安装或正确配置MySQL数据库，请先根据系统环境安装或配置MySQL，再运行程序')

    def mysql_create_table(self, mysql_config, sql):
        """创建MySQL表"""
        import pymysql

        if self.mysql_config:
            mysql_config = self.mysql_config
        mysql_config['db'] = 'weibo'
        connection = pymysql.connect(**mysql_config)
        self.mysql_create(connection, sql)

    def mysql_insert(self, mysql_config, table, data_list):
        """向MySQL表插入或更新数据"""
        import pymysql

        if len(data_list) > 0:
            keys = ', '.join(data_list[0].keys())
            values = ', '.join(['%s'] * len(data_list[0]))
            if self.mysql_config:
                mysql_config = self.mysql_config
            mysql_config['db'] = 'weibo'
            connection = pymysql.connect(**mysql_config)
            cursor = connection.cursor()
            sql = """INSERT INTO {table}({keys}) VALUES ({values}) ON
                     DUPLICATE KEY UPDATE""".format(table=table,
                                                    keys=keys,
                                                    values=values)
            update = ','.join([
                " {key} = values({key})".format(key=key)
                for key in data_list[0]
            ])
            sql += update
            try:
                cursor.executemany(
                    sql, [tuple(data.values()) for data in data_list])
                connection.commit()
            except Exception as e:
                connection.rollback()
                print('Error: ', e)
                traceback.print_exc()
            finally:
                connection.close()

    def weibo_to_mysql(self, wrote_num):
        """将爬取的微博信息写入MySQL数据库"""
        mysql_config = {
            'host': 'localhost',
            'port': 3306,
            'user': 'root',
            'password': '123456',
            'charset': 'utf8mb4'
        }
        # 创建'weibo'表
        create_table = """
                CREATE TABLE IF NOT EXISTS weibo (
                id varchar(10) NOT NULL,
                user_id varchar(12),
                content varchar(2000),
                original_pictures varchar(1000),
                retweet_pictures varchar(1000),
                original BOOLEAN NOT NULL DEFAULT 1,
                video_url varchar(300),
                publish_place varchar(100),
                publish_time DATETIME NOT NULL,
                publish_tool varchar(30),
                up_num INT NOT NULL,
                retweet_num INT NOT NULL,
                comment_num INT NOT NULL,
                PRIMARY KEY (id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"""
        self.mysql_create_table(mysql_config, create_table)
        # 在'weibo'表中插入或更新微博数据
        weibo_list = []
        if len(self.write_mode) > 1:
            info_list = copy.deepcopy(self.weibo[wrote_num:])
        else:
            info_list = self.weibo[wrote_num:]
        for weibo in info_list:
            weibo['user_id'] = self.user_config['user_id']
            weibo_list.append(weibo)
        self.mysql_insert(mysql_config, 'weibo', weibo_list)
        print(u'%d条微博写入MySQL数据库完毕' % self.got_num)

    def update_user_config_file(self, user_config_file_path):
        """更新用户配置文件"""
        with open(user_config_file_path, 'rb') as f:
            lines = f.read().splitlines()
            lines = [line.decode('utf-8') for line in lines]
            for i, line in enumerate(lines):
                info = line.split(' ')
                if len(info) > 0 and info[0].isdigit():
                    if self.user_config['user_id'] == info[0]:
                        if len(info) == 1:
                            info.append(self.user['nickname'])
                            info.append(self.start_time)
                        if len(info) == 2:
                            info.append(self.start_time)
                        if len(info) > 3 and self.is_date(info[2] + ' ' +
                                                          info[3]):
                            del info[3]
                        if len(info) > 2:
                            info[2] = self.start_time
                        lines[i] = ' '.join(info)
                        break
        with codecs.open(user_config_file_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(lines))

    def write_data(self, wrote_num,date):
        """将爬取到的信息写入文件或数据库"""

        if 'csv' in self.write_mode:
            self.write_csv(wrote_num,date)
        if 'txt' in self.write_mode:
            self.write_txt(wrote_num)
        if 'json' in self.write_mode:
            self.write_json(wrote_num)
        if 'mysql' in self.write_mode:
            self.weibo_to_mysql(wrote_num)
        if 'mongo' in self.write_mode:
            self.weibo_to_mongodb(wrote_num)

    def get_weibo_info(self,date):
        """获取微博信息"""
        try:
            start_date = date
            end_date = date

            for hour in range(0, 24):
                hour_end = hour + 1
                url = 'https://s.weibo.com/weibo?q=武汉肺炎&typeall=1&suball=1&timescope=custom:2020-01-%d-%d:2020--01-%d-%d&Refer=g' % (start_date, hour, end_date,hour_end)
                # url = ' https://s.weibo.com/weibo?q=武汉肺炎&typeall=1&suball=1&timescope=custom:2020-01-12-1:2020-01-12-2&Refer=g'
                print('url: ', url)
                selector = self.handle_html(url)
                # self.get_user_info(selector)  # 获取用户昵称、微博数、关注数、粉丝数

                page_num = selector.xpath("//ul[@class = 's-scroll']/li")
                if len(page_num) is 0:
                    page_num = 1
                else:
                    page_num = int(page_num[-1].xpath("a/text()")[0].replace(u"第", '').replace(u"页", ''))
                print(page_num)
                # 获取微博总页数
                wrote_num = 0
                page1 = 0
                random_pages = random.randint(1, 5)
                for page in tqdm(range(1, page_num + 1), desc='Progress'):

                    self.get_one_page(page, url)  # 获取第page页的全部微博

                    if page - page1 == random_pages and page < page_num:
                        sleep(random.randint(6, 10))
                        page1 = page
                        random_pages = random.randint(1, 5)
            # date_start = next_time
            print('添加{}成功'.format(url))
            # wrote_num = self.got_num
            self.write_data(wrote_num, date)  # 将剩余不足20页的微博写入文件

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

    def start(self,date):
        """运行爬虫"""
        try:

            print('*' * 100)
            self.get_weibo_info(date)
            print(u'信息抓取完毕')
            print('*' * 100)

        except Exception as e:
            print('Error: ', e)
            traceback.print_exc()


def main(date):
    try:
        config_path = os.path.split(
            os.path.realpath(__file__))[0] + os.sep + 'config.json'
        if not os.path.isfile(config_path):
            sys.exit(u'当前路径：%s 不存在配置文件config.json' %
                     (os.path.split(os.path.realpath(__file__))[0] + os.sep))
        with open(config_path) as f:
            config = json.loads(f.read())
        wb = Weibo(config)
        wb.start(date)  # 爬取微博信息
    except ValueError:
        print(u'config.json 格式不正确，请参考 '
              u'https://github.com/dataabc/weiboSpider#3程序设置')
    except Exception as e:
        print('Error: ', e)
        traceback.print_exc()


if __name__ == '__main__':
    # datelist = [i for i in range(20191208, 20191232)]
    datelist = [i for i in range(1, 10)]
    for date in datelist:
        main(30)
