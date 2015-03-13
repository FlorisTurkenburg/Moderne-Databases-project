################################################################################
# A B+Tree implementation in Python                                            #
# This code is build on the framework given to us for the Modern Databases     #
# course.                                                                      #         
#                                                                              #
# Authors: Floris Turkenburg, Sander Ginn                                      #
# UvANetID: 10419667, 10409939                                                 #
# Data: March 2015                                                             #
################################################################################


#!env/bin/python
from random import randint
from collections import Mapping, MutableMapping
from sortedcontainers import SortedDict
from encode import encode, decode

max_node_size = 4

class Tree(MutableMapping):
    def __init__(self, max_size=1024):
        self.root = self._create_leaf(tree=self)
        self.max_size = max_size

    @staticmethod
    def _create_leaf(*args, **kwargs):
        return LazyNode(node=Leaf(*args, **kwargs))

    @staticmethod
    def _create_node(*args, **kwargs):
        return Node(*args, **kwargs)

    def _create_root(self, lhs, rhs):
        root = self._create_node(tree=self)
        root.rest = lhs
        root.bucket[min(rhs.bucket)] = rhs
        root._changed = True
        
        return LazyNode(node=root)

    def __getitem__(self, key):
        
        current_node = self.root
        while(hasattr(current_node, "_select")):
            current_node = current_node._select(key)

        if current_node != None:
            return current_node.bucket[key]


    def __setitem__(self, key, value):
        """
        Inserts the key and value into the root node. If the node has been
        split, creates a new root node with pointers to this node and the new
        node that resulted from splitting.
        """

        root_split = self.root._insert(key, value)
        if root_split != None:
            new_root = self._create_root(self.root, root_split)
            self.root = new_root
            print("Created new root")

        pass

    def _commit(self):
        """
        Commit the changes.
        """
        offset = self.root._commit()
        f = open("data", "ba")
        print({"root_offset":offset})
        f.write(encode({"root_offset":offset}))
        f.close()



    def __delitem__(self, key):
        pass

    def __iter__(self):
        pass

    def __len__(self):
        pass

class BaseNode(object):
    def __init__(self, tree):
        self.tree = tree
        self.bucket = SortedDict()
        self._changed = False

    def _split(self):
        """
        Creates a new node of the same type and splits the contents of the
        bucket into two parts of an equal size. The lower keys are being stored
        in the bucket of the current node. The higher keys are being stored in
        the bucket of the new node. Afterwards, the new node is being returned.
        """
        other = self.__class__(self.tree)
        size = len(self.bucket)
        for i in range(int(size/2)):
            key, value = self.bucket.popitem()
            other.bucket[key] = value

        print("New node created: " + str(other))
        return LazyNode(node=other)

    def _insert(self, key, value):
        """
        Inserts the key and value into the bucket. If the bucket has become too
        large, the node will be split into two nodes.
        """

        self.bucket[key] = value
        self._changed = True
        print(str(key)+" inserted into: " + str(self.bucket))
        if len(self.bucket) > max_node_size:
            new_node = self._split()
            new_node._changed = True
            return new_node

        pass

    def _commit(self):
        data = {"type":"Leaf", "entries":self.bucket}
        return(encode(data))


class Node(BaseNode):
    def __init__(self, *args, **kwargs):
        self.rest = None

        super(Node, self).__init__(*args, **kwargs)

    def _select(self, key):
        """
        Selects the bucket the key should belong to.
        """


        if key < min(self.bucket):
            new_node = self.rest
            return new_node

        elif key >= max(self.bucket):
            new_node = self.bucket.values()[-1]
            return new_node

        for i in range(0, len(self.bucket.keys())-1):
            if key >= self.bucket.keys()[i] and key < self.bucket.keys()[i+1]:
                new_node = self.bucket.values()[i]
                return new_node
        
        
        pass

    def _insert(self, key, value):
        """
        Recursively inserts the key and value by selecting the bucket the key
        should belong to, and inserting the key and value into that back. If the
        node has been split, it inserts the key of the newly created node into
        the bucket of this node.
        """

        selected_node = self._select(key)
        print("Node selected: " + str(selected_node.bucket))
        split_node = selected_node._insert(key, value)
        if split_node != None:
            return super()._insert(min(split_node.bucket), split_node)


        pass

    def _commit(self):
        """
        Call the _commit() methods of the children nodes.
        """

        data = {}
        if self.rest != None:
            data["rest"] = self.rest._commit()
        if self.bucket != None:
            for (key, value) in self.bucket.items():
                data[key] = value._commit()

        return encode({"type":"node", "entries":data})



    def _get_data(self):
        """
        Pack the necessary data.
        """
        pass


class Leaf(Mapping, BaseNode):
    def __getitem__(self, key):
        pass

    def __iter__(self):
        pass

    def __len__(self):
        pass


    def _get_data(self):
        """
        Pack the necessary data.
        """
        pass

class LazyNode(object):
    _init = False

    def __init__(self, offset=None, node=None):
        """
        Sets up a proxy wrapper for a node at a certain disk offset.
        """
        self.offset = offset
        self.node = node
        self._init = True

    @property
    def changed(self):
        """
        Checks if the node has been changed.
        """
        if self.node is None:
            return False

        return self.node._changed

    def _commit(self):
        """
        Commit the changes if the node has been changed.
        """
        if not self.changed:
            return

        data = self.node._commit()
        f = open("data", "ba")
        offset = f.tell()
        f.write(data)
        print("data written: " + str(data))
        f.close()

        self._changed = False
        return offset

    def _load():
        """
        Load the node from disk.
        """

        f = open("data", "br")
        i = 0
        while True:
            f.seek(-i,2)
            data = f.read()
            try: 
                footer = decode(data)
                break
            except:
                i += 1

        print(footer)
        print(footer[b'root_offset'])

        pass

    def __getattr__(self, name):
        """
        Loads the node if it hasn't been loaded yet, and dispatches the request
        to the node.
        """
        if self.node == None:
            self.node = self._load()
        
        return getattr(self.node, name)

    def __setattr__(self, name, value):
        """
        Dispatches the request to the node, if the proxy wrapper has been fully
        set up.
        """
        if not self._init or hasattr(self, name):
            return super().__setattr__(name, value)

        setattr(self.node, name, value)



def main():
    tree = Tree()
    for i in range(0, 50):
        tree.__setitem__(randint(0,2000), "value")

    tree.__setitem__(30, "test")
    print(str(tree.__getitem__(30)))
    tree._commit()

    




if __name__ == '__main__':
    main()