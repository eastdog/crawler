__author__ = 'victor'


class Cacher(object):

    def __init__(self, fill_fuc=None):

        self.cach = set() if not fill_fuc else fill_fuc()

    def exist(self, item):

        if item in self.cach:
            return True
        self.cach.add(item)