#!env/bin/python

"""
Author: S.J.R. van Schaik <stephan@synkhronix.com>

Example usage of the basic yamr module.
"""

from yamr import Database, Chunk, Tree

db = Database('test.db', max_size=4)

db[3] = 'foo'
db[5] = 'bar'
db[1] = 'test'
db[7] = 'this'
db[9] = 'that'

print(db[3])

for k, v in db.items():
    print('{}: {}'.format(k, v))

db.commit()
db.close()

