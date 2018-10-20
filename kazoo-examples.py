import time
from kazoo.client import KazooState
from kazoo.client import KazooClient

import logging
logger = logging.getLogger('kazoo_examples')
console = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
console.setFormatter(formatter)
logger.addHandler(console)
logger.setLevel(logging.INFO)


def my_listener(state):
    if state == KazooState.LOST:
        # Register somewhere that the session was lost
        logger.error('session was lost')
    elif state == KazooState.SUSPENDED:
        # Handle being disconnected from Zookeeper
        logger.error('session was suspended')
    elif state == KazooState.CONNECTED:
        logger.info('session is connected')
    else:
        # Handle being connected/reconnected to Zookeeper
        logger.error('session was being connected/reconnected')

zk = KazooClient(hosts='127.0.0.1:2181')
zk.add_listener(my_listener)
zk.start()
id = int(time.time())

if zk.exists('/batches'):
    zk.delete('/batches', recursive=True)

zk.ensure_path('/batches')

@zk.DataWatch('/batches/job_%s/node_1' % id)
def watch_node(data, stat):
    try:
        print("Version: %s, data: %s" % (stat.version, data.decode("utf-8")))
    except Exception as e:
        print(e)

@zk.ChildrenWatch('/batches')
def watch_children(children):
    try:
        print("Children are now: %s" % children)
    except Exception as e:
        print(e)
    # Above function called immediately, and from then on

zk.ensure_path('/batches/job_%s' % id)
zk.create('/batches/job_%s/node_1' % id, b'node 1 online')
zk.create('/batches/job_%s/node_2' % id, b'node 2 online')
zk.create('/batches/job_%s/node_3' % id, b'node 3 online')

data, stat = zk.get('/batches/job_%s/node_1' % id)
assert data.endswith('online'), 'error data=%s' % data
logger.info(data)

data, stat = zk.get('/batches/job_%s/node_2' % id)
assert data.endswith('online'), 'error data=%s' % data
logger.info(data)

data, stat = zk.get('/batches/job_%s/node_3' % id)
assert data.endswith('online'), 'error data=%s' % data
logger.info(data)


key = '/batches/job_%s/node_1' % id
zk.set(key, b'node 1 changed')

key = '/batches/job_%s/node_2' % id
zk.set(key, b'node 2 changed')

key = '/batches/job_%s/node_3' % id
zk.set(key, b'node 3 changed')


key = '/batches/job_%s/node_1' % id
zk.set(key, b'node 1 changed again')

key = '/batches/job_%s/node_2' % id
zk.set(key, b'node 2 changed again')

key = '/batches/job_%s/node_3' % id
zk.set(key, b'node 3 changed again')


