import logging
logging.basicConfig()
from kazoo.client import KazooClient
from kazoo.client import KazooState

def my_listener(state):
	if state == KazooState.LOST:
		print 'lost session'
	elif state == KazooState.SUSPENDED:
		print 'disconnected from Zookeeper'
	elif state == KazooState.CONNECTED:
		print 'connected'

zk = KazooClient(hosts = '127.0.0.1:2181')

zk.add_listener(my_listener)
zk.start()

print 'Hello!!'

zk.stop() 