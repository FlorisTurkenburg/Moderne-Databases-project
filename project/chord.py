import math
from random import randint

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

    def distance(self, lhs, rhs):
        return (self.ring_size + rhs - lhs) % self.ring_size

    def in_range(self, value, lower, upper):
        if lower is upper:
            return True

        return self.distance(lower, value) < self.distance(lower, upper)

    def print_fingers(self):
        print('Finger table for node #{}:'.format(self.node_id))
        print('\n'.join('{}: {}'.format(finger.start, finger.node.node_id) for
            finger in self.fingers))

    def join(self, node):
        self.predecessor = None
        self.successor = node.find_successor(self.node_id)

        pass

    def stabilise(self):
        node = self.successor.predecessor

        if self.in_range(node.node_id, self.node_id+1, self.successor.node_id):
            self.successor = node

        self.successor.notify(self)

        pass

    def notify(self, node):
        if self.predecessor is None or self.in_range(node.node_id, self.predecessor.node_id+1, self.node_id):
            self.predecessor = node

        pass

    def fix_fingers(self):
        i = randint(1, self.finger_count-1)
        self.fingers[i].node = self.find_successor(self.fingers[i].start)

        pass

    def find_successor(self, node_id):
        pred = self.find_predecessor(node_id)
        return pred.successor

        pass

    def find_predecessor(self, node_id):
        pred = self
        while not self.in_range(node_id, pred.node_id+1, pred.successor.node_id+1):
            print(str(node_id), "not in range ("+ str(pred.node_id)+ ", "+str(pred.successor.node_id)+")")
            pred = pred.closest_preceding_node(node_id)

        return pred

    def closest_preceding_node(self, node_id):
        # print(list(range(self.finger_count-1, -1, -1)))
        for i in range(self.finger_count-1, -1, -1):
            print("preceding  index: "+str(i))
            # if self.fingers[i].node.node_id != self.node_id:
            if self.in_range(self.fingers[i].node.node_id, self.node_id+1, node_id):
                print(str(self.fingers[i].node.node_id),"is in range ("+str(self.node_id)+ ", " + str(node_id)+"]")
                return self.fingers[i].node

        return self

    def is_alive(self):
        return self.alive

    def kill(self):
        self.alive = False

