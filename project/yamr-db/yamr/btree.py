"""
Author: S.J.R. van Schaik <stephan@synkhronix.com>

Reference implementation of a hybrid on-disk and in-memory B+ tree with lazily
loaded nodes.
"""

from collections import Mapping, MutableMapping
from msgpack import packb, unpackb
from sortedcontainers import SortedDict

from .chunk import ChunkId

class Tree(MutableMapping):
    def __init__(self, chunk, max_size=1024):
        self.chunk = chunk
        self.max_size = max_size
        self.root = LazyNode(node=Leaf(tree=self), tree=self)

    def commit(self):
        self.root._commit()

    def __getitem__(self, key):
        return self.root[key]

    def __setitem__(self, key, value):
        result = self.root._insert(key, value)

        if result is None:
            return

        key, value = result
        root = LazyNode(node=Node(tree=self), tree=self)
        root.rest = self.root
        root.values[key] = value

        self.root = root

    def __delitem__(self, key):
        raise NotImplementedError

    def __len__(self):
        return len(self.root)

    def __iter__(self):
        for key in self.root:
            yield key

class BaseNode(object):
    def __init__(self, tree, changed=False):
        self.tree = tree
        self.changed = changed
        self.values = SortedDict()

    def _insert(self, key, value):
        self.values[key] = value
        self.changed = True

        if len(self.values) < self.tree.max_size:
            return None

        return self._split()

    def _commit(self):
        pass

class Node(BaseNode, Mapping):
    def __init__(self, tree):
        super().__init__(tree)
        self.rest = None

    def _select(self, key):
        for k, v in reversed(list(self.values.items())):
            if k <= key:
                return v

        return self.rest

    def _insert(self, key, value):
        result = self._select(key)._insert(key, value)
        self.changed = True

        if result is None:
            return

        key, other = result
        return super()._insert(key, other)

    def _split(self):
        other = LazyNode(Node(tree=self.tree, changed=True), tree=self.tree)

        values = self.values.items()
        self.values = SortedDict(values[:len(values) // 2])
        other.values = SortedDict(values[len(values) // 2:])
        
        key, value = other.values.popitem(last=False)
        other.rest = value

        return (key, other)

    def _commit(self):
        self.rest._commit()

        for child in self.values.values():
            child._commit()

        data = packb({
            'rest': self.rest.offset,
            'values': {k: v.offset for k, v in self.values.items()},
        })

        self.tree.chunk.write(ChunkId.Node, data)
        return self.tree.chunk.tell()

    def __getitem__(self, key):
        return self._select(key)[key]

    def __len__(self):
        return sum([len(value) for child in self.values.values() + \
            len(self.rest)])

    def __iter__(self):
        for key in self.rest:
            yield key

        for child in self.values.values():
            for key in child:
                yield key

class Leaf(BaseNode, Mapping):
    def _split(self):
        other = LazyNode(node=Leaf(tree=self.tree, changed=True),
            tree=self.tree)
        
        values = self.values.items()
        self.values = SortedDict(values[:len(values) // 2])
        other.values = SortedDict(values[len(values) // 2:])
        
        return (min(other.values), other)

    def _commit(self):
        data = packb({
            'values': self.values,
        })

        self.tree.chunk.write(ChunkId.Leaf, data)
        return self.tree.chunk.tell()

    def __getitem__(self, key):
        return self.values[key]

    def __len__(self):
        return len(self.values)

    def __iter__(self):
        for key in self.values:
            yield key

class LazyNode(object):
    def __init__(self, tree, offset=None, node=None):
        super().__setattr__('tree', tree)
        super().__setattr__('offset', offset)
        super().__setattr__('node', node)

    def _load_node(self, data):
        data = unpackb(data)

        self.node = Node(tree=self.tree)
        self.node.rest = LazyNode(offset=data[b'rest'], tree=self.tree)
        self.node.values = SortedDict({k: LazyNode(offset=v, tree=self.tree) for
            k, v in data[b'values'].items()})

    def _load_leaf(self, data):
        data = unpackb(data)

        self.node = Leaf(tree=self.tree)
        self.node.values = SortedDict(data[b'values'])

    def _load(self):
        callbacks = {
            ChunkId.Node: self._load_node,
            ChunkId.Leaf: self._load_leaf,
        }

        self.tree.chunk.seek(self.offset)
        callback = callbacks.get(self.tree.chunk.get_id())
        
        if callback:
            callback(self.tree.chunk.read())

    def _commit(self):
        if not self.changed:
            return

        self.offset = self.node._commit()

    def __getattr__(self, name):
        if self.node is None:
            self._load()

        return getattr(self.node, name)

    def __setattr__(self, name, value):
        if name in self.__dict__:
            return super().__setattr__(name, value)

        setattr(self.node, name, value)

    def __getitem__(self, key):
        if self.node is None:
            self._load()

        return self.node[key]

    def __iter__(self):
        if self.node is None:
            self._load()

        yield from self.node.__iter__()

    def __len__(self):
        if self.node is None:
            self._load()

        return len(self.node)

