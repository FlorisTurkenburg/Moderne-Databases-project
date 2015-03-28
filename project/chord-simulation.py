from random import randint, sample
from chord import Node

node_ids = sample(range(Node.ring_size), 5)
node_failures = 2
nodes = []

for node_id in node_ids:
    print('Creating node with node ID: #{}.'.format(node_id))
    node = Node(node_id)
    nodes.append(node)

    if len(nodes) <= 1:
        continue

    known_node = nodes[0]
        
    print('Attempting to join network via node ID: #{}.'.format(
        known_node.node_id))

    node.join(known_node)

    print('Stabilising...')
    
    for _ in range(Node.finger_count):
        for node in nodes:
            node.stabilise()

        for node in nodes:
            node.fix_fingers()

    print('Network:')
    
    for node in nodes:
        node.print_fingers()

for _ in range(node_failures):
    node = nodes[randint(0, len(nodes) - 1)]
    
    print('Killing node #{}...'.format(node.node_id))
    node.kill()

    print('Stabilising...')
    
    for _ in range(Node.finger_count):
        for node in nodes:
            node.stabilise()

        for node in nodes:
            node.fix_fingers()

    print('Network:')
    
    for node in nodes:
        node.print_fingers()

