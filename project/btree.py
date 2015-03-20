################################################################################
# A B+Tree implementation in Python                                            #
# This code is build on the framework given to us for the Modern Databases     #
# course.                                                                      #         
#                                                                              #
# Authors: Floris Turkenburg, Sander Ginn                                      #
# UvANetID: 10419667, 10409939                                                 #
# Date: March 2015                                                             #
################################################################################


#!env/bin/python
import os
from random import randint
from collections import Mapping, MutableMapping
from sortedcontainers import SortedDict
from encode import encode, decode
from checksum import add_integrity, check_integrity

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

        # Delete the lowest key from the right child and put the corresponding
        # value into its rest. This should not happen for leaf nodes, as they do
        # not have a rest.
        print(type(rhs.node))
        if hasattr(rhs, "rest"):
            rhs.node.rest = rhs.bucket[min(rhs.bucket)]
            rhs.node.bucket.pop(min(rhs.bucket))
            print("Rest is: " + str(rhs.rest))
        root.changed = True
        
        return LazyNode(node=root, tree=self)

    def __getitem__(self, key):
        """
        Get the value corresonding with the key.
        """
        print("Searching for key: " + str(key))
        return self.root.__getitem__(key)



    def __setitem__(self, key, value):
        """
        Inserts the key and value into the root node. If the node has been
        split, creates a new root node with pointers to this node and the new
        node that resulted from splitting.
        """

        # If values should be the offsets where the document is written in file:
        value = write_document("data", value)

        root_split = self.root._insert(key, value)
        if root_split != None:
            root_split.node.changed = True
            new_root = self._create_root(self.root, root_split)
            self.root = new_root
            print("Created new root")

        pass

    def _commit(self, filename):
        """
        Commit the changes. Calling the _commit() method of the root node, and
        writing its offset in the footer at the end of the file.
        """
        offset = self.root._commit(filename)
        f = open(filename, "ba")
        f.write(add_integrity(encode({"root_offset":offset, 
                                        "max_size":self.max_size})))
        f.close()

    def _get_documents(self):
        return self.root._get_documents()


    def __delitem__(self, key):
        pass

    def __iter__(self):
        for key in self.root:
            yield key

    def __len__(self):
        pass

class BaseNode(object):
    def __init__(self, tree):
        self.tree = tree
        self.bucket = SortedDict()
        self.changed = False

    def _split(self):
        """
        Creates a new node of the same type and splits the contents of the
        bucket into two parts of an equal size. The lower keys are being stored
        in the bucket of the current node. The higher keys are being stored in
        the bucket of the new node. Afterwards, the new node is being returned.
        """
        other = self.__class__(tree=self.tree)
        size = len(self.bucket)
        for i in range(int(size/2)):
            key, value = self.bucket.popitem()
            other.bucket[key] = value

        print("New node created: " + str(other))
        return LazyNode(node=other, tree=self.tree)

    def _insert(self, key, value):
        """
        Inserts the key and value into the bucket. If the bucket has become too
        large, the node will be split into two nodes.
        """

        self.bucket[key] = value
        self.changed = True
        print(str(key)+" inserted into: " + str(self.bucket))
        if len(self.bucket) > self.tree.max_size:
            new_node = self._split()
            new_node.node.changed = True
            return new_node

        pass

    def _get_data(self, filename):
        """
        Returns the encoded data of the leaf node, containing its type, and the
        key/value pairs. These values will eventually be the offsets of the 
        documents.
        """

        print("Leaf committed: " + str(self) + " bucketsize: " + 
            str(len(self.bucket)))
        data = {"type":"Leaf", "entries":self.bucket}
        print("Leaf data: "+ str(data))
        return(add_integrity(encode(data)))


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
        print("Node selected: " + str(selected_node.bucket) + "\n of type: "+ 
            str(type(selected_node)))
        split_node = selected_node._insert(key, value)
        self.changed = True
        if split_node != None:
            return super()._insert(min(split_node.bucket), split_node)


        pass

    def _get_data(self, filename):
        """
        Call the _commit() methods of the children nodes. And return the encoded
        data of the node, which contains its type, and the offsets of the 
        children nodes.
        """

        data = {}
        if self.bucket != None:
            for (key, value) in self.bucket.items():
                data[key] = value._commit(filename)

        if self.rest != None:
            rest_data = self.rest._commit(filename)
            print("Node committed: " + str(self)+ " bucketsize: " + 
                str(len(self.bucket)))
            print("Node data: " + 
                str({"type":"Node", "rest":rest_data, "entries":data}))
            return add_integrity(encode({"type":"Node", "rest":rest_data, 
                "entries":data}))

        print("Node committed: " + str(self)+ " bucketsize: " + 
            str(len(self.bucket)))
        print("Node data: "+ str({"type":"Node", "entries":data}))
        return add_integrity(encode({"type":"Node", "entries":data}))


    def __getitem__(self, key):
        """
        Recursively call the __getitem__() method, eventually reaching the 
        __getitem__() of a leaf, which returns the matched value, or None if the
        key was not found.
        """
        selected_node = self._select(key)
        return selected_node.__getitem__(key)

    def __iter__(self):
        for key in self.rest:
            yield key

        for child in self.bucket.values():
            for key in child:
                yield key


    def _get_documents(self):
        doc_list = []
        if self.rest != None:
            doc_list += self.rest._get_documents()
        for node in self.bucket.values():
            doc_list += node._get_documents()

        return doc_list


class Leaf(Mapping, BaseNode):
    def __getitem__(self, key):
        """
        Returns the value that corresponds with the key, if the key is not 
        present, None is returned.
        """

        # If values are not offsets of documents, but the document itself, this
        # should be uncommented:
        # if key in self.bucket:
            # return self.bucket[key]
        # return None

        if key in self.bucket:
            offset = self.bucket[key]
            f = open("data", "br")
            i = 0
            while True:
                f.seek(offset)
                data = f.read(i)
                try: 
                    doc_data = decode(check_integrity(data))
                    break
                except:
                    i += 1
            f.close()

            return doc_data.decode("utf-8")

        pass
            

    def __iter__(self):
        for key in self.bucket:
            yield key


    def __len__(self):
        return len(self.bucket)


    def _get_documents(self):
        return list(self.bucket.keys())


class LazyNode(object):
    _init = False

    def __init__(self, offset=None, node=None, tree=None):
        """
        Sets up a proxy wrapper for a node at a certain disk offset.
        """
        self.offset = offset
        self.node = node
        self.tree = tree
        self._init = True

    @property
    def changed(self):
        """
        Checks if the node has been changed.
        """
        if self.node is None:
            return False

        return self.node.changed

    def _commit(self, filename):
        """
        Commit the changes if the node has been changed.
        """
        if not self.changed:
            return self.offset

        data = self.node._get_data(filename)
        f = open(filename, "ba")
        offset = f.tell()
        f.write(data)
        print("Data written: " + str(data))
        print("Written at: " + str(offset))
        f.close()
        self.offset = offset

        self._changed = False
        return offset


    # IT'S FREAKING WORKING
    def _load(self):
        """
        Load the node from disk.
        """

        f = open("data", "br")
        i = 0
        while True:
            f.seek(self.offset)
            data = f.read(i)
            try: 
                node_dict = decode(check_integrity(data))
                break
            except:
                i += 1
        f.close()

        print("Load offset: " + str(self.offset))
        print(node_dict)

        if node_dict[b"type"] == b"Node":
            new_node = Node(tree=self.tree)
            entries = node_dict[b"entries"]
            print(entries)

            for (key, value) in entries.items():
                new_node.bucket[key.decode("utf-8")] = LazyNode(offset=value, tree=self.tree)

            if b"rest" in node_dict:
                new_node.rest = LazyNode(offset=node_dict[b"rest"], tree=self.tree)

            return new_node

        if node_dict[b"type"] == b"Leaf":
            new_leaf = Leaf(tree=self.tree)
            entries = node_dict[b"entries"]

            for (key, value) in entries.items():
                new_leaf.bucket[key.decode("utf-8")] = value

            return new_leaf


     
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

    def __iter__(self):
        if self.node is None:
            self._load()

        yield from self.node.__iter__()


def create_initial_tree(): 
    tree = Tree(max_size=4)
    for i in range(0, 15):
        tree.__setitem__(randint(0,2000), "value")

    tree.__setitem__(30, "test")
    # print(str(tree.__getitem__(30)))
    tree._commit("data")
    
    

# Retrieve the latest footer, it will also retrieve the footer if it is not the
# last data in the file (but still the last footer), unless the data after the
# footer is not decodeable (incomplete data, could occur when a write to the
# file was not succesfully finished)
def get_last_footer():
    f = open("data", "br")
    i = 0
    read_till = 0

    while True:
        try:
            f.seek(-i,2)
        except OSError:
            print("Could not retrieve footer, file is incorrect")
            return None
        
        data = f.read(i-read_till)
        try: 
            footer = decode(check_integrity(data))
            print(footer)
            if b"root_offset" in footer:
                break
            else:
                read_till = i

        except:
            i += 1

    
    print(footer)
    print(footer[b"root_offset"])
    return footer
    
def write_document(tofile, data):
    f = open(tofile, "ba")
    offset = f.tell()
    print("offset: ", str(offset))
    f.write(add_integrity(encode(data)))
    f.close()
    return offset


def insert_document():
    tree = Tree(max_size=4)

    doc_offset = write_document("data", "Dit is een test document")
    key = "Testkey"
    tree.__setitem__(key, doc_offset)
    tree._commit("data")



def compaction(tree):
    doc_keys = tree._get_documents()

    new_tree = Tree(max_size=tree.max_size)
    for key in doc_keys:
        document_data = tree.__getitem__(key)

        newfile = open("newdata", "ba")
        write_offset = newfile.tell()
        newfile.write(add_integrity(encode(document_data)))
        newfile.close()

        new_tree.__setitem__(key, write_offset)

    new_tree._commit("newdata")

    os.rename("newdata", "data")


# Load the tree if there is one stored on disk, else create a new one.
def start_up(max_size):
    footer = get_last_footer()
    if footer == None:
        print("No existing tree was found. Creating a new one..")
        return Tree(max_size=max_size)

    tree = Tree(max_size=footer[b"max_size"])
    tree.root = LazyNode(offset=footer[b"root_offset"], tree=tree)
    return tree



def main():
    # Create a new tree if needed:
    # create_initial_tree()
    
    # Load the tree from disk and perform some tests, like inserting a new key
    # or retrieving a key.

    tree = start_up(max_size=4)
    

    tree["dockey"] = "testdoc" 
    tree["foo"] = "this"
    tree["bar"] = "is"
    tree["what"] = "for"
    tree["up"] = "testing"

    print("all keys: ", str(tree._get_documents()))
    tree._commit("data")

    # compaction(new_tree)
    # print("Get document: ", str(new_tree.__getitem__(b"Testkey")))

    # print("Value of key", repr("30"),"is:", str(new_tree.__getitem__(30)))
    
    # new_tree.__setitem__(666, "insert_test")
    # new_tree._commit()






if __name__ == '__main__':
    main()