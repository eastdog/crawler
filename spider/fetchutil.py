#coding: utf-8
__author__ = 'victor'

import random
import urllib2
import time
import socket
import mechanize
from bs4 import BeautifulSoup
from multiprocessing import Pool
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

TARGET_TEST_WEBSITE = 'http://www.douban.com'

class UserAgent:

    agents = [
        ("User-Agent", "DreamPassport/3.0; isao/DiGiRa"),
        ("User-Agent", "ANTFresco/3.xx "),
        ("User-Agent", "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1)"),
        ("User-Agent", "Mozilla/5.0 (Windows NT 6.3; Win64; x86) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2049.0 Safari/537.36"),
        ("User-Agent", "Mozilla/5.0 (compatible; U; ABrowse 0.6; Syllable) AppleWebKit/420+ (KHTML, like Gecko)"),
        ("User-Agent", "Mozilla/5.0 (compatible; ABrowse 0.4; Syllable)"),
        ("User-Agent", "Mozilla/5.0 (compatible; MSIE 9.0; AOL 9.7; AOLBuild 4243.19; Windows NT 6.1; WOW64; Trident/5.0; FunWebProducts)"),
        ("User-Agent", "Mozilla/5.0 (Windows XP) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1995.67 Safari/537.36"),
        ("User-Agent", "Mozilla/5.0 (Windows XP) AppleWebKit/(KHTML, like Gecko) Chrome/31.0.1680.16 Safari/537.36"),
        ("User-Agent", "Mozilla/5.0 (Windows 8) AppleWebKit/537.36 Chrome/28.0.1468.0 Safari/537.36"),
        ("User-Agent", "Mozilla/5.0 (Windows XP; X64) AppleWebKit/537.36 (KHTML) Chrome/27.0.1453.93 Safari/537.36"),
        ("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_2) AppleWebKit/537.17 (KHTML, like Gecko) Chrome/24.0.1309.0 Safari/537.17"),
        ("User-Agent", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/53.5 (KHTML, like Gecko) Chrome/19.0.1084.9 Safari/536.5"),
        ("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_3) AppleWebKit/535.20 (KHTML, like Gecko) Chrome/19.0.1036.7 Safari/535.20"),
        ("User-Agent", "Mozilla/5.0 (Windows NT 5.1; rv:31.0) Gecko/20100101 Firefox/31.0"),
        ("User-Agent", "Mozilla/5.0 (Windows NT 6.2; rv:22.0) Gecko/20130405 Firefox/23.1"),
        ("User-Agent", "Mozilla/5.0 (X11; Ubuntu; Linux x86_64) Gecko/20130331 Firefox/21.0"),
        ("User-Agent", "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:11.0) Gecko Firefox/11.0"),
        ("User-Agent", "IBM WebExplorer /v0.85"),
        ("User-Agent", "Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US) AppleWebKit/532.4 (KHTML, like Gecko) Maxthon/3.0.6.27 Safari/532.4"),
        ("User-Agent", "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.0; Trident/4.0; Maxthon; SV1; .NET CLR 1.1.4322; .NET CLR 2.4.84947; SLCC1; Media Center PC 4.0; Zune 3.5; Tablet PC 3.5; InfoPath.3)"),
        ("User-Agent", "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0; Maxthon; .NET CLR 3.5.307; FDM)"),
        ("User-Agent", "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0; Maxthon/3.0)"),
        ("User-Agent", "Mozilla/5.0 (Windows; U; Windows NT 6.1; rv:2.2) Gecko/20110201"),
        ("User-Agent", "Mozilla/5.0 (X11; ; Linux i686; rv:1.9.2.20) Gecko/20110805"),
        ("User-Agent", "Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.9.0.14) Gecko/2009091010"),
        ("User-Agent", "Mozilla/5.0 (Windows; U; Win 9x 4.90; SG; rv:1.9.2.4) Gecko/20101104 Netscape/9.1.0285"),
        ("User-Agent", "Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.8.1.7pre) Gecko/20070815 Firefox/2.0.0.6 Navigator/9.0b3"),
        ("User-Agent", "Mozilla/5.0 (Windows; U; MSIE 9.0; WIndows NT 9.0; en-US))"),
        ("User-Agent", "Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; Trident/6.0)"),
        ("User-Agent", "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 7.1; Trident/5.0)"),
        ("User-Agent", "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0; yie8)"),
        ("User-Agent", "Mozilla/4.0(compatible; MSIE 7.0c; Windows NT 6.0)"),
        ("User-Agent", "Mozilla/4.0 (compatible; MSIE 6.2; Windows XP)"),
        ("User-Agent", "Mozilla/4.0 (compatible; MSIE 6.1; Windows XP; .NET CLR 1.1.4322;"),
    ]

    sources = [
        'http://www.baidu.com',
        'http://www.xiami.com',
        'http://www.douban.com'
    ]

    @classmethod
    def get_user_agent(cls):
        return cls.agents[random.randint(0, len(cls.agents)-1)]

    @classmethod
    def get_source(cls):
        return cls.sources[random.randint(0, len(cls.sources)-1)]

def cloak_url(urlstr):

    request = urllib2.Request(urlstr)
    request.add_header('Referer', UserAgent.get_source())
    request.add_header("Accept-Language", "zh-cn")
    request.add_header("Accept", 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8')
    request.add_header("Connection", "keep-alive")
    request.add_header("User-Agent", UserAgent.get_user_agent()[1])
    return request

def __fetch_candis(urlstr, numtries=1):

    try:
        from driver import Browser
        # response = mechanize.urlopen(urlstr)
        br = Browser.browser
        br.get(urlstr)
        html = br.page_source
        # print html
        soup = BeautifulSoup(html)

    except Exception, e:
        print 'fail to hear back from original page %s times'%numtries
        print urlstr
        print e
        time.sleep(10)
        if numtries==1:
            return __fetch_candis(urlstr, numtries+1)
        else:
            return False

    # print html
    rawlist = soup.find('table', {'id':'ip_list'}).find_all('tr')[1:]
    print 'fetch proxy webpage done'
    candis = []

    for tr in rawlist:
        tds = tr.find_all('td')
        if tds[6].get_text().lower()=='http' and float(tds[7].find('div')['title'][:-2])<4:
            candis.append('%s:%s'%(tds[2].get_text(), tds[3].get_text()))

    br.quit()
    return candis

def test_proxy(proxy, tries=1):

    global TARGET_TEST_WEBSITE
    start = time.time()
    try:
        # browser.set_proxies({'http': proxy})
        # browser.open('http://www.douban.com/')
        request = mechanize.Request(TARGET_TEST_WEBSITE)
        request.set_proxy(proxy, 'http')
        mechanize.urlopen(request)
    except socket.timeout:
        return (proxy, 200)
    except urllib2.URLError, e:
        if tries==1:
            return test_proxy(proxy, 2)
        else:
            # print e
            return (proxy, 200)
    except Exception, e:
        # print e
        return (proxy, 200)

    used = time.time()-start

    if used<5:
        time.sleep(5-used)

    return (proxy, used)

def get_proxy(num=1, url='http://www.xici.net.co/nt', showtime=False, speed=2):

    socket.setdefaulttimeout(120)
    pool = Pool(processes=4)

    candis = __fetch_candis(url)
    results = pool.map(test_proxy, candis)
    pool.terminate()
    results = filter(lambda x:x[1]<speed, results)
    if showtime:
        return sorted(results, key=lambda x:x[1])[:num]
    results = map(lambda x:x[0], sorted(results, key=lambda x:x[1])[:num])
    print 'got %s proxies'%len(results)
    return results