# -*- coding:utf-8 -*-
__author__ = 'victor'

import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import re
import datetime
from abc import abstractmethod
from bs4 import BeautifulSoup
from chardet import detect


class BaseProcessor(object):

    def __init__(self):
        self.status = None

    @abstractmethod
    def parse(self):
        """
        get page(s) to follow
        """
        pass

    @abstractmethod
    def extract(self):
        """
        extract information needed
        """
        pass

    def process(self, url, response):

        # self.status = response.getcode()
        html = response.read()
        soup = BeautifulSoup(html)

        nexts = self.parse(html, soup, url)
        result = self.extract(html, soup, url)
        return result, nexts

    @abstractmethod
    def process404(self):
        """
        define how to process 404 urls
        """
        pass


class Processor_hs(BaseProcessor):

    """
    retrieve full html page, follow urls of songs
    """

    def extract(self, html, soup, url):
        return html

    def parse(self, html, soup, url):
        ids = self.follow.findall(html)
        return set(map(lambda x: 'http://www.xiami.com/song/%s'%x.strip(), ids))

    def __init__(self):
        self.follow = re.compile(r'href="/song/(\d+)')


class ProcessorIwjw(BaseProcessor):

    cp = re.compile(r'p(\d{1,2})$')

    def extract(self, html, soup, url):
        if u'很抱歉，没有找到符合条件的房源' not in unicode(html):
            return html
        else:
            return None

    def parse(self, html, soup, url):

        if u'很抱歉，没有找到符合条件的房源' in unicode(html):
            return None
        current_page = url.split('/')[5].strip()
        pn = self.cp.findall(current_page)[0]
        return [url.replace('p%s' % pn, 'p%s' % (int(pn)+1))]


class Processor_hn(BaseProcessor):

    """
    retrive full html pages, don't follow
    """

    def extract(self, html, soup, url):
        return html

    def parse(self, html, soup, url):
        return


class Processor_hpsina(BaseProcessor):

    """
    retrieve full html pages, follow previous date
    """
    newsurl = re.compile(r'href=/([\w/]+/\d{4}-\d{2}-\d{2}/[\d\w-]+?.shtml)')

    def extract(self, html, soup, url):

        if 'oldnews' in url:
            return
        return html
        # coding = detect(html).get('encoding')
        # try:
        #     return html.decode(coding or 'gb2312').encode('utf-8')
        # except:
        #     return html.decode(coding or 'utf-8').encode('utf-8')

    def parse(self, html, soup, url):

        if 'oldnews' in url:
            try:
                urls = map(lambda x: 'http://ent.sina.com.cn/%s'%x, self.newsurl.findall(html))
            except:
                urls = []
            urls.append(self._previous_date(url))
            return urls
        return

    def _previous_date(self, url):
        y, m, d = url.split('-')
        y, m, d = map(int, [y[-4:], m, d[:2]])
        return 'http://ent.sina.com.cn/music/oldnews/%s.shtml'%\
               (datetime.date(y, m, d) - datetime.timedelta(days=5)).isoformat()

    def process404(self, url):
        return None, [self._previous_date(url)]


class Processor_jdhp(BaseProcessor):

    """
    retrvie full html pages of jd, follow next page
    """