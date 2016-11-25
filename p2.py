import time
import sys
import socket
import os
import threading

# global variables
serversocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
MY_IP = ''
MY_SERVER_PORT = ''
MY_NODE_NAME = ''
#--------------------------------------------------------------
MY_ID = ''
ID_list = []
PORT_list = []
host = "localhost"
is_leader = 0
leader = 0;
holdingElection = False
#--------------------------------------------------------------
# end of global var


# function for sending message to a specified node
def writeToServer(message, server, port):
	clientSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	clientSocket.connect((server, port))
	clientSocket.send(message)
	clientSocket.close()


#--------------------------------------------------------------
def check_leader_exists(ID, p):
	# global leader
	ns = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	try:
		ns.connect((host, int(p)))
		ns.send('Are you the leader?')
		msg = ns.recv(2048)
		if msg == 'YES':
			leader = int(ID)
		ns.close()
	except:
		print('Could not connect to the process')
#--------------------------------------------------------------
def check_leader():
	while True:
		if leader != 0 and is_leader == 0 and holdingElection == False:
			ns = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			try:
				ns.connect((host, leader))
				ns.send('try')
				msg = recv(2048)
				ananlyze_msgs(None, None, msg = msg)
				ns.close()
			except socket.error as e:
				print('Cannot find the leader, start new election!')
				new_election()

#--------------------------------------------------------------
# To initiate election
def new_election():
	# global MY_ID
	# global holdingElection
	# global is_leader

	holdingElection = true;
	candidates = []
	for p in ID_list:
		if p > MY_ID:
			candidates.append(p)
	NO_RESPONSE = True

	for c in candidates:
		ns = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		ns.settimeout(4) # Set the time T unit to receive OK messages from these processes
		try:
			ns.connect((host, c))
			ns.send('ELECTION')
			msg = ns.recv(2048)
			if msg == 'OK':
				print('OK received')
				NO_RESPONSE = False
			ns.close()
		except socket.error as e:
			print('Fail to communucate election')

	if NO_RESPONSE == True:
		is_leader = 1
		leader = MY_ID # leader = self
		holdingElection = False
		sent_coordinator() # send COORDINATOR to all processes with lower IDs


#--------------------------------------------------------------
# Send coordinator message to all processes with lower IDs
def sent_coordinator():
	# global MY_ID
	# global holdingElection
	print('I am the coordinator')

	for p in ID_list:
		if p < MY_ID:
			ns = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			ns.settimeout(5) # Set the time T' unit to receive COORDINATOR message
			try: 
				ns.connect((host, c))
				ns.send('COORDINATOR {}'.format(MY_ID))
				ns.close
			except socket.error as e:
				print('Failed yo send COORDINATOR')

#--------------------------------------------------------------
def answer_socket(conn, addr):
	msg = conn.recv(2048)
	ananlyze_msgs(conn, addr, msg)

#--------------------------------------------------------------
# function that verify all msgs and do the following operations:
def ananlyze_msgs(conn, addr, msg):
	# global holdingElection
	# global is_leader
	# global leader
	if msg == 'try':
		if is_leader == 1: 
			conn.send('alive')
	elif msg == 'alive':
		print('leader is here!')
	elif msg == 'ELECTION': # On receiving ELECTION from process q with lower ID
		conn.send('OK')
		if (holdingElection == False):
			new_election()
	elif msg == "Are you the leader?":
		if is_leader == 1:
			conn.send('YES')
		else:
			conn.send('NO')
	elif 'COORDINATOR' in msg: # On receiving COORDINATOR from q
		print(msg)
		msplit = msg.split()
		leader = int(msplit[1])
		if MY_ID > leader and holdingElection == False:
			new_election()
		else:
			holdingElection = False
			# cancel any onging election
	else:
		pass
#--------------------------------------------------------------


# main operations
MY_NODE_NAME = raw_input("What is node ID? \n")
MY_ID = int(MY_NODE_NAME)
f = open("ips.txt")

for ips in f:
    ips = ips.split()
    ID_list.append(int(ips[0]))
    PORT_list.append(int(ips[2]))
    if ips[0] == MY_NODE_NAME:
    	MY_SERVER_PORT = int(ips[2])

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((host, MY_SERVER_PORT))

if is_leader == 0:
	for i, p in zip(ID_list, PORT_list):
		check_leader_exists(i,p)

s.listen(5)

thread_check_leader = threading.Thread(target=check_leader, args=())
thread_check_leader.daemon = True
thread_check_leader.start()


if is_leader == 0:
	print("The leader is: {}".format(leader))
while True:
	conn, addr = s.accept()
	new_thread = threading.Thread(target=answer_socket, args=(conn, addr))
	new_thread.daemon = True
	new_thread.start()




    



	    
    
    













