#!/usr/bin/python

from collections import OrderedDict

class IndexedPQ(object):
    def __init__(self):
        super(IndexedPQ, self).__init__()
        self.ipq = OrderedDict()

    def isEmpty(self):
        return len(self.ipq) == 0

    def size(self):
        return len(self.ipq)

    def insert(self, key, val):
        self.ipq[key] = val
        self.ipq = OrderedDict(sorted(self.ipq.iteritems(), cmp=self._cmp, key=lambda d:d))

    def change(self, key, val):
        if not key in self.ipq:
            raise ValueError("Invalid key: %s"%key)
        else:
            self.ipq[key] = val
            self.ipq = OrderedDict(sorted(self.ipq.iteritems(), cmp=self._cmp, key=lambda d:d))

    def delete(self, key):
        del self.ipq[key]

    def minIndex(self):
        return self.ipq.keys()[0]

    def min(self):
        return self.values()[0]

    def delMin(self):
        indexOfMin = self.ipq.keys()[0]
        del self.ipq[indexOfMin]
        return indexOfMin

    def show(self):
        return self.ipq.items()

    def keys(self):
        return self.ipq.keys()

    def _cmp(self, a, b):
        if a[1] < b[1]:
            return -1
        elif a[1] > b[1]:
            return 1
        else:
            if a[0] < b[0]:
                return -1
            else:
                return 1
