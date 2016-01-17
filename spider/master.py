__author__ = 'victor'

import time
import os
import codecs
import json
import random
import Queue
import processor as ps
import fetchutil
from fetcher import *
from cach import Cacher

import sys
reload(sys)
sys.setdefaultencoding('utf-8')


class Master(object):

    def __init__(self, rest_period=5, result_model=None, result_dir=None):

        self.stime = rest_period
        self.queue = Queue.Queue(maxsize=200000)
        self.fethcers = []
        self.avalb_proxy = []
        self.cach = None

        self.dir = 'results' if result_dir==None else result_dir
        if not os.path.isdir(self.dir): os.makedirs(self.dir)

        if result_model == 'html':
            self._record = self._html_write
        else:
            self.count = 0
            self.max = 1000 #1000 results writen into 1 file
            self.fobject = codecs.open(os.path.join(self.dir, '%s_%s_%s_%s_%s' % time.localtime()[:5]), 'w', 'utf-8')
            self._record = self._default_write

    def _html_write(self, result, fname):
        # with codecs.open(os.path.join(self.dir, fname+'.html'), 'w', 'utf-8') as fo:
        #     fo.write(result)
        with open(os.path.join(self.dir, fname+'.html'), 'w') as fo:
            fo.write(result)

    def _default_write(self, result, fname=None):

        self.count += 1
        if self.count > self.max:
            self.count = 0
            self.fobject.close()
            self.fobject = codecs.open(os.path.join(self.dir, '%s_%s_%s_%s_%s'%time.localtime()[:5]), 'w', 'utf-8')

        self.fobject.write(result)

    def _add2follow(self, *urls):
        for url in urls:
            if not self.queue.full():
                self.queue.put(url)

    def add_fetchers(self, *fetchers):
        for ft in fetchers:
            self.fethcers.append(ft)
            print 'new fetcher attached:', ft.proxy

    def _init_queue(self, seeds):
        for seed in seeds:
            if self.queue.full():
                return
            self.queue.put(seed)

    def _get_wake_fetcher(self):

        for index, ft in enumerate(self.fethcers):
            if (time.time() - ft.cach_time) > self.stime:
                if ft.broken:
                    print 'fetcher %s is gone' % ft.proxy
                    del self.fethcers[index]
                    continue
                return ft
        # print 'all fetchers are busy'
        time.sleep(random.random()+random.randint(self.stime/3, self.stime-1))
        return self._get_wake_fetcher()

    def get_avaliable_proxy(self, fail=0):

        if fail>2:
            return
        if len(self.avalb_proxy) == 0:
            print 'fetching some proxies'
            self.avalb_proxy = fetchutil.get_proxy(-1, url='http://www.xici.net.co/nt')

        occupied = set(ft.proxy for ft in self.fethcers)
        while len(self.avalb_proxy)>0:
            proxy = self.avalb_proxy.pop(0)
            if not proxy in occupied:
                return (proxy, 'http')

        return self.get_avaliable_proxy(fail+1)

    def _init_cach(self, cach_func=None):
        self.cach = Cacher(cach_func)

    def start(self, seeds, cach_func=None):

        print 'start to crawl'
        print 'there are %s fetchers, they are' % len(self.fethcers)
        print ','.join([str(ft.proxy) for ft in self.fethcers])

        self._init_queue(seeds)
        self._init_cach(cach_func)
        count, failed = 0, 0
        while not self.queue.empty():

            if count > 100:
                print 'have processed 100 pages', time.ctime()
                print 'failed, %s pages' % failed
                count = 0
                failed = 0

            seed = self.queue.get()

            #try to avoid dup
            if self.cach and (self.cach.exist(seed.strip())):
                continue

            fetcher = self._get_wake_fetcher()
            count += 1
            print 'processing', seed, time.ctime()

            try:
                result, nexts = fetcher.fetch(seed)
                if nexts:
                    self._add2follow(*nexts)
                if result:
                    self._record(result, seed.split('/')[4].strip())
                # if 'oldnews' in seed:
                #     print 'date: ', seed.split('/')[-1]
                #     if len(filter(lambda x: 'oldnews' in x, nexts)) == 0:
                #         print 'seeds may exhaust'
            # except ProcessException, pe:
            #     # print seed, 'process error'
            #     fetcher.update_failure(self)
            #     self.queue.put(seed)
            except Exception, e:
                fetcher.update_failure(self)
                self.queue.put(seed)
                failed += 1
                print e
                print seed, 'fetch error'
            fetcher.tick()


def generate_fetchers(num, master):

    for _ in xrange(num):
        # fetcher = Fetcher(proxy=master.get_avaliable_proxy(), processor=ps.Processor_hpsina())
        fetcher = Fetcher(proxy=master.get_avaliable_proxy(), processor=ps.Processor_hpsina())
        master.add_fetchers(fetcher)


def init_cach():
    return set(f.split('.')[0].strip() for f in os.listdir('G:/htmls'))


if __name__ == '__main__':

    master = Master(rest_period=5, result_model='html', result_dir='G:/music_news/sina')
    fetcher_a = Fetcher(processor=ps.Processor_hpsina())
    master.add_fetchers(fetcher_a)
    generate_fetchers(8, master)

    master.start(['http://ent.sina.com.cn/music/oldnews/2012-08-14.shtml',
                  'http://ent.sina.com.cn/music/oldnews/2012-08-13.shtml',
                  'http://ent.sina.com.cn/music/oldnews/2012-08-12.shtml',
                  'http://ent.sina.com.cn/music/oldnews/2012-08-11.shtml',
                  'http://ent.sina.com.cn/music/oldnews/2012-08-10.shtml'])


    # master = Master(rest_period=6, result_model='html', result_dir='G:/htmls')
    # fetcher_a = Fetcher(processor=ps.Processor_hn())
    # master.add_fetchers(fetcher_a)
    # generate_fetchers(6, master)
    # indir = 'F:/Projects/Crawlers/xiami/data/music_info'
    # for f in os.listdir(indir):
    #     ids = set()
    #     if f.endswith('utf8'):
    #         print 'working in ', f
    #         with codecs.open(os.path.join(indir, f), 'r', 'utf-8') as fin:
    #             for line in fin:
    #                 ids.add(json.loads(line).keys()[0])
    #         master.start(map(lambda x: 'http://www.xiami.com/song/%s'%x.strip(), ids), init_cach)