import helper_connect, helper, time, thread, json, socket
helper.setProgramState("recover")
MY_ID = int(raw_input("What is my node name? \n"))
(MY_IP,MY_PORT) = helper.getIPandPort(MY_ID)
MY_IP = socket.gethostname()
LEADER = helper.findLeader()
TOTAL_NODES = 3
MAJORITY = 3
ServerSockets = [None] * (TOTAL_NODES+1)
FILES = dict()
ACK_COUNT = dict()
WAIT_FOR = []
TRANSACTION_LIST = dict()
FIRST_SYNC = True
EPOCH = 0
FAILED_TRANSACTION = []

# ------------------------------ #
# leader election variables:
holdingElection = ''
is_leader = ''
leader = ''
id_port = {}              # hashtable that holds {id, port number}
# ------------------------------ #

#more helper functions
def createServerSockets():
	serversocket = helper_connect.createServer(MY_IP,MY_PORT)
	while 1:
		(serversoc, address) = serversocket.accept()
		global ServerSockets
		print "Connection established with node" + str(helper.getID(address[0]))
		ServerSockets[helper.getID(address[0])] = serversoc
		helper.sendSync(ServerSockets[helper.getID(address[0])])

# create thread to listen on cli
# 	do things, send things on nw
def listenOnCLI():
	global FILES
	while 1:
		command = raw_input("Enter command \n")
		fileName = raw_input("Enter File Name \n")
		if (helper.getProgramState() != "ready"):
			print "Service unavailable, please try later"
			continue

		if (command == "read"):
			print helper.handleCliRead(fileName, FILES, ClientCon, MY_ID)
		if (command == "create"):
			contents = raw_input("Enter content \n")
			helper.handleCliCreate(fileName, contents, ClientCon, MY_ID)
		if (command == "append"):
			contents = raw_input("Enter content \n")
			helper.handleCliAppend(fileName, contents, ClientCon, MY_ID)
		if (command == "delete"):
			helper.handleCliDelete(fileName,ClientCon, MY_ID)
def listenOnNetwork():
	global FILES
	global TRANSACTION_LIST
	global WAIT_FOR
	global FIRST_SYNC
	while 1:
		mydata = ClientCon.recv(10*1024)
		mydata = mydata.split("|")
		for ldata in mydata:
			try:
				data = json.loads(ldata)
			except:
				continue
			# try:	
			# 	data = json.loads(data)
			# except:
			# 	print "SERVER FAILURE DETECTED please try back later"
			# 	time.sleep(1)
			# 	continue
			if(data["command"] == "proposal"):
				print "received proposal, sending ack"
				try:
					data["originalRequest"]["fileContents"]
				except:
					data["originalRequest"]["fileContents"]=""
				saveCommand = str(data["transactionID"][0]) + "," + str(data["transactionID"][1]) + "," +data["originalRequest"]["command"] + "," + data["originalRequest"]["fileName"] + "," + data["originalRequest"]["fileContents"]
				TRANSACTION_LIST[str(data["transactionID"][0]) + str(data["transactionID"][1])] = saveCommand
				helper.writeLog(json.dumps(data))
				WAIT_FOR.append(str(data["transactionID"][0]) + str(data["transactionID"][1]))
				helper.replyAck(data, ClientCon, MY_ID)
			
			if(data["command"] == "commit"):
				print "received commit"
				tID = str(data["transactionID"][0]) + str(data["transactionID"][1])
				if(WAIT_FOR[0] == tID):
					print "deliver tID"
					operation = TRANSACTION_LIST[tID]
					FILES = helper.executeOP(operation, FILES)
					del WAIT_FOR[0]

			if(data["command"] == "synchronise"):
				print "Entering sync mode"
				helper.setProgramState("sync")
				otherlog = data["log"].split()
				mylog = open("log.txt").read().split()

				# if(FIRST_SYNC == True):
				# 	for i in range (0, len(otherlog)):
				# 		newEntries = newEntries + otherlog[i] + "\n"
				# 		FILES = helper.executeOP(otherlog[i], FILES)
				# 		FIRST_SYNC = False
				# 		helper.setProgramState("ready")
				# 		print ("Sync successful")
				# 		continue
				# print "do i ever reach here?"
				if(FIRST_SYNC == True):
					print "ONLY ONCE ONLY ONCE ONLY ONCE"
					helper.eraseOwnLog()
					for line in otherlog:
						FILES = helper.executeOP(line, FILES)
						helper.writeSyncLog(line)
					helper.setProgramState("ready")
					FIRST_SYNC = False
					print ("Sync successful")
					continue



				if(len(mylog) == len(otherlog)):
					helper.setProgramState("ready")
					print ("Sync successful")
				if(len(mylog) < len(otherlog)):
					for i in range (len(mylog), (len(otherlog) - len(mylog) + 1) ):
						helper.writeSyncLog(otherlog[i])
						FILES = helper.executeOP(otherlog[i], FILES)
						try:
							WAIT_FOR.remove(str(otherlog[i][0]) + str(otherlog[i][1]))
						except:
							pass

				if(len(WAIT_FOR) != 0):
					for line in mylog:
						line1 = line.split(",")
						if (str(line1[0])+str(line1[1]) == WAIT_FOR[0]):
							print "Sync has delivered a message"
							FILES = helper.executeOP(line, FILES)
							del WAIT_FOR[0]

				helper.setProgramState("ready")


				# differentLine = 999
				# for i in range(0,len(mylog)):
				# 	if (mylog[i] != otherlog [i]):
				# 		differentLine = i
				# 		break
				# if(differentLine == 999 and le):
				# 	continue
				# for i in range (differentLine, len(otherlog)):
				# 	FILES = helper.executeOP(otherlog[i], FILES)
				# 	helper.writeSyncLog(json.dumps(otherlog[i]))
				helper.setProgramState("ready")
				print ("Sync successful")


			if(data["command"] == "fail"):
				print ("A transaction id " + str(data["transactionID"]) + " has failed. Please try again later")
				tID = str(data["transactionID"][0]) + str(data["transactionID"][1])
				# WAIT_FOR.remove(tID)	
			# leader election part:
			# ---------------------------------------------------------------- #
			if (data["command"] == "try"):
				if is_leader == 1:
					print ('alive')
					ClientCon.send('alive')
			if (data["command"] == "alive"):
				print('leader is alive')
			if 'Are you the leader?' in data["command"]:
				msplit = data["command"].split()
				asker = msplit[1]
				if is_leader == 1:
					if asker > MY_ID and holdingElection == False:
						is_leader = 0
						leader = asker
						new_election()
						ClientCon.send('You are bigger than me')
					else:
						ClientCon.send('Yes')
				else:
					ClientCon.send('No'.encode)
			if (data["command"] == "election"):
				ClientCon.send('OK')
				if holdingElection == False:
					new_election()
			if 'COORDINATOR' in data["command"]:
				msplit = data["command"].split()
				is_leader = 0
				leader = int(msplit[1])
				print 'The leader is ', leader
				if MY_ID > leader and holdingElection == False:
					new_election()
				else:
					holdingElection = False
			else:
				pass
			# ---------------------------------------------------------------- #

def timerThread():
	global FAILED_TRANSACTION
	global ServerSockets
	time.sleep(5)
	if (len(FAILED_TRANSACTION) != 0):
		helper.sendFail(ServerSockets, FAILED_TRANSACTION.pop())
def listenServerOnNetwork(followerID):
	global ACK_COUNT
	global EPOCH
	global FAILED_TRANSACTION
	global WAIT_FOR
	while 1:
		
		try:
			data = ServerSockets[followerID].recv(10*1024)
		except:
			print "Could not establish connection with " + str(followerID)
			time.sleep(1)
			continue
		# if(len(FAILED_TRANSACTION) != 0):
		# 		helper.sendFail(ServerSockets,FAILED_TRANSACTION.pop())
		try:
			mydata = data.split("|")
		except:
			print "Follower CRASH DETECTED Please try again in a while" + str(followerID)
			time.sleep(1)
			continue
		for data in mydata:
			try:
				data = json.loads(data)
			except:
				continue
			if (data["command"] == "create" or data["command"] == "append" or data["command"] == "delete" or data["command"] == "read"):
				print "sending sync"
				sender = data["sender"]
				helper.sendSync(ServerSockets[LEADER])
				if(sender != LEADER):
					helper.sendSync(ServerSockets[sender])
			if (data["command"] == "create" or data["command"] == "append" or data["command"] == "delete"):
				print "received request, sending proposal"
				transaction_number = helper.getTransactionNumber()
				helper.setTransactionNumber(int(transaction_number)+1)
				transactionID = [EPOCH,int(transaction_number)+1]
				strTransID = str(transactionID[0]) + str(transactionID[1])
				ACK_COUNT [strTransID] = 0
				newData = {}
				newData ["originalRequest"] = data
				newData ["command"] = "proposal"
				newData["sender"] = MY_ID
				newData["transactionID"] = transactionID
				newData = json.dumps(newData)
				newData = newData + "|"
				for i in range(1,len(ServerSockets)):
					result = helper.sendProposal(newData, ServerSockets[i])
					result = result.split(",")
					if(result[1] not in FAILED_TRANSACTION):
						FAILED_TRANSACTION.append(result[1])
				thread.start_new_thread(timerThread,())
		
			if(data["command"] == "ack"):
				print "received ack"
				transactionID = str(data["transactionID"][0]) + str(data["transactionID"][1])
				ACK_COUNT[transactionID]+=1
				if(ACK_COUNT[transactionID] >= MAJORITY):
					print "sending commit"
					helper.sendCommit(data["transactionID"], ServerSockets)
					FAILED_TRANSACTION.pop()
		    # leader election part:
			# ---------------------------------------------------------------- #
			if (data["command"] == "try"):
				if is_leader == 1:
					print ('alive')
					ServerSockets[followerID].send('alive')
			if (data["command"] == "alive"):
				print('leader is alive')
			if 'Are you the leader?' in data["command"]:
				msplit = data["command"].split()
				asker = msplit[1]
				if is_leader == 1:
					if asker > MY_ID and holdingElection == False:
						is_leader = 0
						leader = asker
						new_election()
						ServerSockets[followerID].send('You are bigger than me')
					else:
						ServerSockets[followerID].send('Yes')
				else:
					ServerSockets[followerID].send('No'.encode)
			if (data["command"] == "election"):
				ServerSockets[followerID].send('OK')
				if holdingElection == False:
					new_election()
			if 'COORDINATOR' in data["command"]:
				msplit = data["command"].split()
				is_leader = 0
				leader = int(msplit[1])
				print 'The leader is ', leader
				if MY_ID > leader and holdingElection == False:
					new_election()
				else:
					holdingElection = False
			else:
				pass
			# ---------------------------------------------------------------- #

# New election part:
# =====================================================================================
def new_election():
	global holdingElection
	global is_leader
	global id_port

	holdingElection = True

	candidates = []

	for i in id_port:
		if i > MY_ID:
			candidates.append(i)

	NO_RESPONSE = True

	for c in candidates:
			ServerSockets[c].send('ELECTION')
			msg = ServerSockets[c].recv(2048)
			if msg == 'OK':
				print('OK received')
				NO_RESPONSE = False

	if NO_RESPONSE == True:
		is_leader = 1
		leader = MY_ID
		holdingElection = False
		# if no response received, send COORDINATOR to all processes with lower IDs
		send_coordinator()

#-------------------------------------------------------------- 
# send COORDINATOR to all processes with lower IDs     
def send_coordinator():
    global holdingElection
    global id_port
    print('I am the leader')

    for i in id_port:
        if i < MY_ID:
            ServerSockets[i].send('COORDINATOR {}'.format(MY_ID))
#--------------------------------------------------------------
# function used for checking whether there is existing leader 
# in all processes, if the current process is larger than the exiting
# leader, initiate a new election
# parameter: pair {id, port number}
def check_leader(p):
    global leader
    tmp_id = p
    tmp_port = id_port[p]

    ns = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    ns.settimeout(1)
    try:
        ns.connect((host, tmp_port))
        ns.send('Are you the leader? {}'.format(MY_ID))
        msg = ns.recv(2048)
        if msg == 'YES':
            leader = tmp_id
        elif msg == 'You are bigger than me':
            leader = MY_ID
        ns.close()
    except:
        print('Could not connect to the process')
#--------------------------------------------------------------
# function check whether the current leader is alive
# if not, start a new election
def check_leader_alive():
    global holdingElection
    global id_port

    while True:
        time.sleep(randint(5,15))
        if leader != 0 and is_leader == 0 and holdingElection == False:
            ns = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            ns.settimeout(2)
            try:
                ns.connect((host, id_port[leader]))
                ns.send('try')
                print('try')
                print(datetime.now())
                msg = ns.recv(2048)
                analyze_msg(None,None,msg=msg)
                ns.close()
            except socket.error as e:
                print('Did not find the leader, starting a new election...')
                new_election()
#--------------------------------------------------------------

def get_leader():
	global is_leader
	global leader
	global MY_ID
	global id_port

	f = open("ips.txt")

	for ips in f:
		ips = ips.split()
		id_port[int(ips[0])] = int(ips[2])
		if MY_ID == int(ips[0]):
			MY_PORT = int(ips[2])
	s = ServerSockets[MY_ID]

	if is_leader == 0:
		for p in id_port:
			try:
				check_leader(p)
			except:
				print('connection failed')

	# if no leader found or no other processes is alive
	# then I am the leader
	if leader == 0:
		is_leader = 1
		leader = MY_ID
		print('I\'m the leader')
	s.listen(5)

	thread_check_leader = threading.Thread(target=check_leader_alive, args=())
	thread_check_leader.daemon = True
	thread_check_leader.start()

	#while True:

		# I don't know how to write this part

# ===================================================================================== 
# End of new election part

#Update with own log

#create sockets for all communications 

if (LEADER == MY_ID):
	thread.start_new_thread(createServerSockets, ())
	time.sleep(1)
(IP,PORT) = helper.getIPandPort(LEADER)
if (LEADER == MY_ID):
	IP = socket.gethostname()
if (LEADER == MY_ID):
	for i in range(0,TOTAL_NODES):
		thread.start_new_thread(listenServerOnNetwork,(i + 1,))


try:
	ClientCon = helper_connect.connectTo(IP,PORT)
except:
	print "Could not connect to server"
helper.setProgramState("ready")


try:
	thread.start_new_thread(listenOnCLI, ())
	thread.start_new_thread(listenOnNetwork, ())
except:
	print "Thread start error"
while 1:
   time.sleep(0.5)


