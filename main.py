import helper_connect, helper, time, thread, json, socket
helper.setProgramState("recover")
MY_ID = int(raw_input("What is my node name? \n"))
(MY_IP,MY_PORT) = helper.getIPandPort(MY_ID)
MY_IP = socket.gethostname()
LEADER = helper.findLeader()
TOTAL_NODES = 5
MAJORITY = 3
ServerSockets = []
FILES = dict()
ACK_COUNT = dict()
WAIT_FOR = []
TRANSACTION_LIST = dict()
FIRST_SYNC = True
EPOCH = 0
PROPOSED_TRANSACTIONS = dict()
CONNECTED_CLIENTS = []
#more helper functions
def createServerSockets():
	serversocket = helper_connect.createServer(MY_IP,MY_PORT)
	while 1:
		(serversoc, address) = serversocket.accept()
		global ServerSockets
		print "Connection established with node" 
		ServerSockets.append(serversoc)
		thread.start_new_thread(listenServerOnNetwork,(serversoc,))
		helper.sendSync(serversoc)

# create thread to listen on cli
# 	do things, send things on nw
def clientRecvThread(sock):
	while 1:
		clientData = sock.recv(512)
		response = listenOnCLI(clientData)
		try:
			sock.send(response)
		except:
			sock.send("working")
def clientAcceptThread():
	serversocket = helper_connect.createServer(socket.gethostname(), 8082)
	while 1:
		(serversoc,address) = serversocket.accept()
		global CONNECTED_CLIENTS
		print "Connected to client"
		CONNECTED_CLIENTS.append(serversoc)
		thread.start_new_thread(clientRecvThread,(serversoc,))


def listenOnCLI(clientData):
	global FILES
	global ClientCon
	clientData = clientData.split(",")
	command = clientData[0]
	fileName = clientData[1]
	if (helper.getProgramState() != "ready"):
		return "Service not in ready state, please try later"

	if (command == "read"):
		return helper.handleCliRead(fileName, FILES, ClientCon, MY_ID)
	if (command == "create"):
		contents = clientData[2]
		helper.handleCliCreate(fileName, contents, ClientCon, MY_ID)
	if (command == "append"):
		contents = clientData[2]
		helper.handleCliAppend(fileName, contents, ClientCon, MY_ID)
	if (command == "delete"):
		helper.handleCliDelete(fileName,ClientCon, MY_ID)
def listenOnNetwork():
	global FILES
	global TRANSACTION_LIST
	global WAIT_FOR
	global FIRST_SYNC
	global CONNECTED_CLIENTS
	global ClientCon
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
				# helper.writeLog(json.dumps(data))
				# WAIT_FOR.append(str(data["transactionID"][0]) + str(data["transactionID"][1]))
				helper.replyAck(data, ClientCon, MY_ID)
			
			if(data["command"] == "commit"):
				print "received commit"
				tID = str(data["transactionID"][0]) + str(data["transactionID"][1])
				# if(WAIT_FOR[0] == tID):
				print "deliver tID"
				operation = TRANSACTION_LIST[tID]
				FILES = helper.executeOP(operation, FILES)
				helper.writeLog(operation)
				helper.announceCommit(CONNECTED_CLIENTS, TRANSACTION_LIST[tID])
					# del WAIT_FOR[0]

			if(data["command"] == "synchronise"):
				print "Entering sync mode"
				helper.setProgramState("sync")
				otherlog = ""
				shouldRead = True
				try:
					otherlog = data["log"].split()
				except:
					shouldRead = False

				if(FIRST_SYNC == True and shouldRead == True and len(otherlog)>=1):
					print 
					helper.eraseOwnLog()
					print "LENGTH"
					print len(otherlog) 
					for line in otherlog:
						print line
						FILES = helper.executeOP(line, FILES)
						helper.writeSyncLog(line)
				helper.setProgramState("ready")
				FIRST_SYNC = False
				print ("Sync successful")

			if(data["command"] == "fail"):
				print ("A transaction id " + str(data["transactionID"]) + " has failed. Please try again later")
				tID = str(data["transactionID"][0]) + str(data["transactionID"][1])
				helper.announceFailure(CONNECTED_CLIENTS, TRANSACTION_LIST[tID])
				# WAIT_FOR.remove(tID)	
def serverTimerThread():
	global PROPOSED_TRANSACTIONS
	global ServerSockets
	
	while 1:
		for transactionID,transactionTime in PROPOSED_TRANSACTIONS.iteritems():
			if (int(time.time()) - transactionTime > 5):
				helper.sendFail(ServerSockets,transactionID)
				del PROPOSED_TRANSACTIONS[transactionID]
		time.sleep(3)
def listenServerOnNetwork(conObj):
	global ACK_COUNT
	global EPOCH
	global PROPOSED_TRANSACTIONS
	global WAIT_FOR
	global ServerSockets
	while 1:
		
		try:
			data = conObj.recv(10*1024)
		except:
			print "Could not establish connection server " 
			time.sleep(1)
			continue
		try:
			mydata = data.split("|")
		except:
			print "Incomplete message received" 
			time.sleep(1)
			continue
		for data in mydata:
			try:
				data = json.loads(data)
			except:
				continue
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
				print "sending proposals"
				for i in range(0,len(ServerSockets)):
					result = helper.sendProposal(newData, ServerSockets[i])
					result = result.split(",")
					if(result[1] not in PROPOSED_TRANSACTIONS):
						PROPOSED_TRANSACTIONS[result[1]] = int(time.time())
		
			if(data["command"] == "ack"):
				print "received ack"
				transactionID = str(data["transactionID"][0]) + str(data["transactionID"][1])
				ACK_COUNT[transactionID]+=1
				if(ACK_COUNT[transactionID] >= MAJORITY and (transactionID in PROPOSED_TRANSACTIONS)):
					print "sending commit"
					del PROPOSED_TRANSACTIONS[transactionID] 
					helper.sendCommit(data["transactionID"], ServerSockets)


#Update with own log

#create sockets for all communications 

if (LEADER == MY_ID):
	thread.start_new_thread(createServerSockets, ())
	time.sleep(1)
(IP,PORT) = helper.getIPandPort(LEADER)
if (LEADER == MY_ID):
	IP = socket.gethostname()
if (LEADER == MY_ID):
	# for i in range(0,TOTAL_NODES):
	# 	thread.start_new_thread(listenServerOnNetwork,(i+1,))
	thread.start_new_thread(serverTimerThread,())

thread.start_new_thread(clientAcceptThread,())

try:
	ClientCon = helper_connect.connectTo(IP,PORT)
	print "created client con"
except:
	print "Could not connect to server"
helper.setProgramState("ready")


try:
	thread.start_new_thread(listenOnNetwork, ())

except:
	print "Thread start error"
while 1:
   time.sleep(0.5)