import math
from random import randint
from collections import deque

debug = False

class Finger(object):
    def __init__(self, start, node):
        self.start = start
        self.node = node

class Node(object):
    ring_size = 2 ** 5
    finger_count = int(math.log(ring_size, 2))

    @property
    def successor(self):
        return self.fingers[0].node

    @successor.setter
    def successor(self, value):
        self.fingers[0].node = value

    def __init__(self, node_id):
        self.alive = True
        self.node_id = node_id
        self.predecessor = self
        self.fingers = [Finger((node_id + 2 ** k) % self.ring_size, self) for k
            in range(self.finger_count)]

        self.successor_list = deque()

    def distance(self, lhs, rhs):
        return (self.ring_size + rhs - lhs) % self.ring_size


    """
    The method returns True for a value in range [lower, upper),
    if lower=upper, the full range of the ring is covered, so True is returned.
    """
    def in_range(self, value, lower, upper):
        if lower is upper:
            return True

        return self.distance(lower, value) < self.distance(lower, upper)

    def print_fingers(self):
        if not self.is_alive():
            print('\033[1;31mFinger table for node #{}:\033[1;m'.format(self.node_id))
            print('\n'.join('\033[1;31m{}: {}\033[1;m'.format(finger.start, 
                               finger.node.node_id) for finger in self.fingers))
        
        else:
            print('Finger table for node #{}:'.format(self.node_id))
            print('\n'.join('{}: {}'.format(finger.start, finger.node.node_id) for
                finger in self.fingers))

    def join(self, node):
        self.predecessor = None
        self.successor = node.find_successor(self.node_id)
        succ = self.successor
        for i in range(3):
            succ = succ.successor
            if succ.is_alive():
                self.successor_list.append(succ)
            else:
                break

        pass

    """
    node.node_id in range (self.node_id, self.successor.node_id)
    """
    def stabilise(self):
        # if not self.is_alive():
        #     print(str(self.node_id)+" is dead")
        #     return
        # print(str(self.node_id)+" survived")

        node = self.successor.predecessor

        if self.node_id != node.node_id and \
            self.in_range(node.node_id, self.node_id+1, self.successor.node_id):
            
            self.successor = node

        if debug:
            print("\033[1;32mNotify of node: "+str(self.node_id)+"to node: "+
                    str(self.successor.node_id)+"\033[1;m")
        self.successor.notify(self)

        self.refresh_successor_list()

        succ = self.successor
        while not succ.is_alive():
            try:
                succ = self.successor_list.popleft()
            except IndexError:
                break


        if debug and self.node_id == 8:
            print("Succ of 8 = "+str(succ.node_id))
        self.successor = succ
        self.successor.notify(self)
        self.refresh_successor_list()

        if debug and self.node_id == 1:
            print("\033[1;33mNode: 1 \033[1;m")
            print("\033[1;33mSuccessor: "+str(self.successor.node_id)+
                  " \nSuccesor_list: "+str([succ.node_id for succ in 
                                            self.successor_list])+"\033[1;m")
        
        if debug and self.node_id == 8:
            print("\033[1;36mNode: 8 \033[1;m")
            print("\033[1;36mSuccessor: "+str(self.successor.node_id)+
                  " \nSuccesor_list: "+str([succ.node_id for succ in 
                                            self.successor_list])+"\033[1;m")



        pass

    def refresh_successor_list(self):
        self.successor_list.clear()
        succ = self.successor
        for i in range(3):
            succ = succ.successor
            if succ.is_alive():
                self.successor_list.append(succ)
            else:
                break

        pass


    """
    node.node_id in range (predecessor.node_id, self.node_id)
    """
    def notify(self, node):
        # if not self.is_alive():
        #     return

        if self.predecessor is None or self.in_range(node.node_id, 
                                    self.predecessor.node_id+1, self.node_id):
            
            self.predecessor = node
            if debug:
                print("\033[1;34mPredecessor of "+str(self.node_id)+" is now "
                        +str(node.node_id)+"\033[1;m")

        pass

    def fix_fingers(self):
        # if not self.is_alive():
        #     return

        # i = randint(1, self.finger_count-1)
        for i in range(1,self.finger_count):
            self.fingers[i].node = self.find_successor(self.fingers[i].start)

        pass

    def find_successor(self, node_id):
        pred = self.find_predecessor(node_id)
        return pred.successor


    """
    node_id not in range (pred.node_id, pred.successor.node_id]
    """
    def find_predecessor(self, node_id):
        pred = self
        while not self.in_range(node_id, pred.node_id+1, pred.successor.node_id+1):
            if debug:
                print(str(node_id), "not in range ("+ str(pred.node_id)+ ", "
                        +str(pred.successor.node_id)+"]")

            pred = pred.closest_preceding_node(node_id)

        if debug:
            print("Predecessor of "+str(node_id)+" is: "+str(pred.node_id))
        return pred

    """
    finger[i].node in range (self.node_id, node_id)
    """
    def closest_preceding_node(self, node_id):
        for i in range(self.finger_count-1, -1, -1):
            if self.in_range(self.fingers[i].node.node_id, self.node_id+1, node_id):
                if debug:
                    print(str(self.fingers[i].node.node_id),"is in range ("
                            +str(self.node_id)+ ", " + str(node_id)+")")
                
                return self.fingers[i].node

        return self

    def is_alive(self):
        return self.alive

    def kill(self):
        self.alive = False

