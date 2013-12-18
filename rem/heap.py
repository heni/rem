import logging


class PriorityQueue(object):
    def __init__(self):
        getattr(super(PriorityQueue, self), "__init__")()
        self.revIndex = {}
        self.objects = []
        self.values = []

    @classmethod
    def heapify(cls, objects, values):
        heap = cls()
        assert len(objects) == len(values)
        heap.objects = objects[:]
        heap.values = values[:]
        for i in xrange(len(heap) / 2, -1, -1):
            heap.pushdown(i)
        for i, o in enumerate(heap.objects):
            heap.revIndex[o] = i
        return heap

    def copy(self):
        return self.heapify(self.objects[:], self.values[:])

    def swap(self, i, j):
        if i != j:
            self.objects[i], self.objects[j] = self.objects[j], self.objects[i]
            self.values[i], self.values[j] = self.values[j], self.values[i]
            if self.revIndex:
                self.revIndex[self.objects[i]] = i
                self.revIndex[self.objects[j]] = j

    def pushdown(self, i):
        n = len(self.objects)
        while 2 * i + 1 < n:
            child = 2 * i + 1
            if 2 * i + 2 < n and self.values[2 * i + 2] < self.values[child]: child = 2 * i + 2
            if self.values[i] <= self.values[child]:
                break
            self.swap(i, child)
            i = child
        return i

    def rollup(self, i):
        while i > 0:
            parent = (i - 1) / 2
            if self.values[parent] <= self.values[i]:
                break
            self.swap(i, parent)
            i = parent
        return i

    def add(self, object, value):
        if self.revIndex.has_key(object):
            logging.warning("%r already is in heap", object)
            self.changeValue(object, value)
            return
        self.objects.append(object)
        self.values.append(value)
        pos = len(self.objects) - 1
        self.revIndex[object] = pos
        self.rollup(pos)

    def pop(self, obj=None):
        f = self.revIndex.get(obj, None) if obj else 0
        if f is None:
            return
        n = len(self.objects)
        self.swap(f, n - 1)
        retObject = self.objects.pop()
        retVal = self.values.pop()
        del self.revIndex[retObject]
        if n - 1 != f:
            self.pushdown(self.rollup(f))
        return retObject, retVal

    def peak(self):
        return self.objects[0], self.values[0]

    def changeValue(self, object, value):
        pos = self.revIndex.get(object, None)
        if value == 0:
            if pos is not None:
                self.swap(pos, len(self.objects) - 1)
                self.values.pop()
                self.objects.pop()
                del self.revIndex[object]
                if pos < len(self.objects):
                    self.pushdown(self.rollup(pos))
        elif pos is None:
            self.add(object, value)
        else:
            old = self.values[pos]
            self.values[pos] = value
            if value > old:
                self.pushdown(pos)
            elif value < old:
                self.rollup(pos)

    def __len__(self):
        return len(self.objects)

    def __nonzero__(self):
        return bool(self.objects)

    def __contains__(self, obj):
        return obj in self.revIndex

    def __iter__(self):
        return self.objects.__iter__()

    def items(self):
        return zip(self.objects, self.values)

