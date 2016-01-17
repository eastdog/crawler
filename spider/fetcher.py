__author__ = 'victor'

import socket
import time
import urllib2
import fetchutil


class ProcessException(Exception):

    def __init__(self, e):
        self.e = e


class FetchException(Exception):
    pass


class Fetcher(object):

    def __init__(self, model='default', proxy=None, processor=None):

        self.model = model
        self.id = id(self)
        self.proxy = proxy
        self.cach_time = time.time()
        self.last_failure = time.time()
        self.workload = 0
        self.failure = 0
        self.broken = False

        self.queue = None
        self.processor = processor

    def set_proxy(self, proxy):
        self.proxy = proxy

    def set_processor(self, processor):
        self.processor = processor

    def associate_queue(self, queue):
        self.queue = queue

    def _fetch(self, url):

        socket.setdefaulttimeout(300)
        request = fetchutil.cloak_url(url)
        if self.proxy:
            request.set_proxy(*self.proxy)

        if self.model == 'default':
            try:
                return urllib2.urlopen(request)
            except urllib2.HTTPError, he:
                return he.code
            except socket.timeout:
                # print 'fetching time out', url
                return False
            except Exception, e:
                # print 'fetching failed', url, e.message
                return False
        else:
            print 'model undefined'
            return False

    def fetch(self, url):

        response = self._fetch(url)
        self.workload += 1
        if not response:
            raise FetchException()
        if response == 404:
            return self.processor.process404(url)
        try:
            return self.processor.process(url, response)
        except Exception, e:
            raise ProcessException(e)

    def tick(self):

        self.cach_time = time.time()
        # self.avaliability = False
        # time.sleep(stime)
        # self.avaliability = True

    def drop(self):

        self.broken = True

    def update_failure(self, master):

        if not self.proxy:
            return

        self.failure += 1
        if (time.time()-self.last_failure) > 20:
            self.failure -= 1
        if self.failure > 20:
            self.proxy = master.get_avaliable_proxy()
            if not self.proxy:
                self.drop()
            self.failure = 0