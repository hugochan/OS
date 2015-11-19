#!/usr/bin/python

class IndexMinPQ(object):
    """docstring for resizing IndexMinPQ:
    does not support repeated indices (k)
    """
    def __init__(self):
        self.__pq = [0] # store indices (k)
        self.__qp = {None:0} # store {index:sequence num}
        self.__keys = {} # store {index:element}

    def isEmpty(self):
        return len(self.__pq) - 1 == 0

    def size(self):
        return len(self.__pq) - 1

    def constains(self, k):
        return k in self.__qp

    def insert(self, k, key):
        self.__pq.append(k)
        N = self.size()
        self.__qp[k] = N
        self.__keys[k] = key
        self.__swim(N)

    def change(self, k, key):
        self.__keys[k] = key
        t = self.__qp.get(k)
        if t is None:
            print "Change Error"
        else:
            self.__swim(t)
            self.__sink(t)

    def delete(self, k):
        t = self.__qp.get(k)
        if t is None:
            print "Change Error"
        else:
            self.__exch(t, self.size())
            del self.__pq[-1]
            del self.__qp[k]
            del self.__keys[k]
            self.__swim(t)
            self.__sink(t)

    def minIndex(self):
        return self.__pq[1]

    def min(self):
        return self.__keys[self.__pq[1]]

    def delMin(self):
        indexOfMin = self.__pq[1]
        self.__exch(1, self.size())
        del self.__pq[-1]
        del self.__qp[indexOfMin]
        del self.__keys[indexOfMin]
        self.__sink(1)
        return indexOfMin

    def show(self):
        for i in range(1, self.size()+1):
            print str(self.__pq[i]) + ":" + str(self.__keys[self.__pq[i]])

    def keys(self):
        # to be implemented
        return self.__pq[1:]

    def __swim(self, k):
        while k > 1 and self.__more(k/2, k):
            self.__exch(k/2, k)
            k = k/2

    def __sink(self, k):
        N = self.size()
        while 2*k <= N:
            j = 2*k
            if j < N and self.__more(j, j+1):
                j += 1
            if self.__less(k, j):
                break
            self.__exch(k, j)
            k = j

    def __more(self, i, j):
        return self.__keys[self.__pq[i]] > self.__keys[self.__pq[j]] # to be generalized

    def __less(self, i, j):
        return self.__keys[self.__pq[i]] < self.__keys[self.__pq[j]] # to be generalized

    def __exch(self, i, j):
        self.__pq[i], self.__pq[j] = self.__pq[j], self.__pq[i]
        self.__qp[self.__pq[i]], self.__qp[self.__pq[j]] = i, j

if __name__ == '__main__':
    import random
    k = range(10)
    keys = range(100, 110)
    random.shuffle(k)
    random.shuffle(keys)
    _dict = dict(zip(k, keys))
    print _dict
    impq = IndexMinPQ()
    for each_k, each_key in _dict.items():
        impq.insert(each_k, each_key)
    impq.show()
    impq.change(4, 99)
    impq.show()
    impq.delete(0)
    impq.show()
    for i in range(5):
        print impq.delMin()
    impq.show()
    print impq.constains(9)
    print impq.min()
    print impq.minIndex()
