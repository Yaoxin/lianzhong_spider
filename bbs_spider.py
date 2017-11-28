# coding:utf-8
# @author cloudy <liufuyun88@gmail.com>
# @created Mon Nov 27 2017 23:14:52 GMT+0800 (CST)

import logging
import urlparse
import threading
import traceback
import requests
import pymongo
import threadpool
from bs4 import BeautifulSoup

logging.basicConfig(
    level=logging.DEBUG,
    format=
    '[%(asctime)s][%(levelname)s] %(filename)s::%(funcName)s(%(lineno)d): %(message)s'
)


def safestr(obj, encoding='utf-8'):
    is_iter = lambda x: x and hasattr(x, 'next')
    if isinstance(obj, unicode):
        return obj.encode(encoding)
    elif is_iter(obj):
        return map(safestr, obj)
    else:
        return str(obj)


class HTMLDoweloader(object):
    @staticmethod
    def download(url):
        if not url:
            return None

        headers = {
            'User-Agent':
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/537.36 (KHTML, like Gecko) '
            'Chrome/60.0.3112.90 Safari/537.36'
        }

        resp = requests.get(url, headers=headers)
        if resp.status_code == 200:
            logging.debug("[%s] downloading url(%s) successful.",
                          threading.current_thread(), url)
            resp.encoding = 'utf-8'
            return resp.text

        return None


class HTMLParser(object):
    @staticmethod
    def parser_list(html_url, html_cnt):
        if not html_url or not html_cnt:
            return
        soup = BeautifulSoup(html_cnt, 'lxml')
        return HTMLParser._get_list_data(html_url, soup)

    @staticmethod
    def parser_detail(html_url, html_cnt):
        if not html_url or not html_cnt:
            return
        soup = BeautifulSoup(html_cnt, 'lxml')
        return HTMLParser._get_detail_data(html_url, soup)

    @staticmethod
    def _get_list_data(page_url, soup):
        data = []
        obj = soup.find("table", id="threadlist")
        sub_objs = obj.find_all("tbody", recursive=False)
        for item in sub_objs:
            subject = item.find("th", class_="subject")
            if not subject:
                continue
            link = subject.a["href"].strip()
            title = subject.a.get_text().strip()

            info = item.find("td", class_="by")
            author = info.cite.get_text().strip()
            create_tm = info.em.string.strip()

            num = int(item.find("td", class_="num").a.string)
            urls = []
            if num < 50:
                urls.append(urlparse.urljoin(page_url, link))
            else:
                for i in range(0, (num / 50) + 1):
                    link_sep = link.rsplit(".", 1)
                    new_link = "{0}-{1}.{2}".format(link_sep[0], i + 1,
                                                    link_sep[1])
                    urls.append(urlparse.urljoin(page_url, new_link))
            data.append({
                "title": title,
                "author": author,
                "create_tm": create_tm,
                "urls": urls
            })

        return data

    @staticmethod
    def _get_detail_data(page_url, soup):
        data = {}
        data['url'] = page_url
        data['msg_list'] = []
        obj = soup.find("div", id="postsContainer")
        sub_objs = obj.find_all("table", recursive=False)
        for item in sub_objs:
            author = item.find("td", class_="postauthor")
            poster = author.find("div", class_="poster")
            d_uid = poster.p.string.strip()
            d_name = ""
            next_siblings = poster.find_next_siblings()
            for sub in next_siblings:
                if sub.name == "p":
                    d_name = sub.get_text().split("document")[0].strip()
                    break

            post_content = item.find("td", class_="postcontent")
            post_title = post_content.find("div", class_="topictitle")
            if post_title is not None:
                d_title = post_title.h1.get_text().strip().split("\n")[0]
            else:
                d_title = None

            post_pi = post_content.find("div", class_="pi")
            post_info = post_pi.find("div", class_="postinfo")
            d_time = post_info.find("em").get_text().strip().split(" ")[1]

            post_message = post_content.find("div", class_="postmessage")
            d_content = post_message.get_text().strip()

            data["msg_list"].append({
                "uid": d_uid,
                "name": d_name,
                "post_time": d_time,
                "post_title": d_title,
                "post_content": d_content
            })

        return data


class DataStore(object):
    def __init__(self, ip, port, db_name):
        self.db = pymongo.MongoClient(ip, port)[db_name]

    def store_data(self, data):
        if not data:
            return

        collection = self.db["siguo_bbs"]
        collection.insert(data)
        logging.debug("[%s] store artile(%s) successful!!!",
                      threading.current_thread(), data["title"])


def deal_fun(article_info, store):
    urls = article_info.get('urls', None)
    if not urls:
        logging.warning("[%s]: urls is null.", threading.currentThread())
        return

    try:
        article_info["msg_list"] = []
        for url in urls:
            resp_cnt = HTMLDoweloader.download(url)
            if not resp_cnt:
                logging.warning("[%s]: download url(url) failed.",
                                threading.currentThread())

            out = HTMLParser.parser_detail(url, resp_cnt)
            article_info["msg_list"].extend(out["msg_list"])
    except Exception, e:
        logging.error("[%s] %s %s", threading.current_thread(), str(e),
                      traceback.format_exc())
        return

    # 写入时不需要关心时序，所以可以不加锁
    store.store_data(article_info)


def main():
    # 主线程爬list_page 子线程爬detail_page, 然后子线程负责入库

    # 爬取前看看此论坛分类的总页面数
    page_nums = 420
    store = DataStore("localhost", 27017, "lianzhong")
    pool = threadpool.ThreadPool(20)
    base_url = "http://bbs.lianzhong.com/showforum.aspx?forumid=14&typeid=-1&page={}"

    for i in range(3, page_nums + 1):
        url = base_url.format(i)
        resp_cnt = HTMLDoweloader.download(url)
        if not resp_cnt:
            logging.warning("download url(%s) failed.", url)
            continue
        articles = HTMLParser.parser_list(url, resp_cnt)
        data = [((article, store), {}) for article in articles]
        reqs = threadpool.makeRequests(deal_fun, data)
        for req in reqs:
            pool.putRequest(req)

    pool.wait()


if __name__ == '__main__':
    main()
