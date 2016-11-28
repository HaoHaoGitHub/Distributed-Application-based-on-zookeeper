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

#more helper functions
def createServerSockets():
	serversocket = helper_connect.createServer(MY_IP,MY_PORT)
	while 1:
		(serversoc, address) = serversocket.accept()
		global ServerSockets
		print "Connection established with node" + str(helper.getID(address[0]))
		ServerSockets[helper.getID(address[0])] = serversoc

# create thread to listen on cli
# 	do things, send things on nw
def listenOnCLI():
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
	while 1:
		data = ClientCon.recv(100 * 1024)
		data = json.loads(data)
		if(data["command"] == "proposal"):
			print "received proposal, sending ack"
			try:
				data["originalRequest"]["fileContents"]
			except:
				data["originalRequest"]["fileContents"]=""
			saveCommand = str(data["transactionID"][0]) + "," + str(data["transactionID"][1]) + "," +data["originalRequest"]["command"] + "," + data["originalRequest"]["fileName"] + "," + data["originalRequest"]["fileContents"]
			TRANSACTION_LIST[str(data["transactionID"][0]) + str(data["transactionID"][1])] = saveCommand
			helper.writeLog(data)
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

			differentLine = 999
			for i in range(0,len(mylog)):
				if (mylog[i] != otherlog [j]):
					differentLine = i
					break
			newEntries = ""
			for i in range (differentLine, len(otherlog)):
				newEntries = newEntries + otherlog[i] + "\n"
				FILES = helper.executeOP(otherlog[i])
			helper.writeLog(newEntries)
			helper.setProgramState("ready")
			print ("Sync successful")

def listenServerOnNetwork(followerID):
	global ACK_COUNT
	while 1:
		try:
			data = ServerSockets[followerID].recv(100 * 1024)
		except:
			print "Could not establish connection with " + str(followerID)
			time.sleep(1)
			continue
		data = json.loads(data)
		if (data["command"] == "create" or data["command"] == "append" or data["command"] == "delete" or data["command"] == "read"):
			print "sending sync"
			helper.sendSync(ServerSockets)
		if (data["command"] == "create" or data["command"] == "append" or data["command"] == "delete"):
			print "received request, sending proposal"
			transaction_number = helper.getTransactionNumber()
			helper.setTransactionNumber(int(transaction_number)+1)
			transactionID = [MY_ID,int(transaction_number)+1]
			strTransID = str(transactionID[0]) + str(transactionID[1])
			ACK_COUNT [strTransID] = 0
			newData = {}
			newData ["originalRequest"] = data
			newData ["command"] = "proposal"
			newData["sender"] = MY_ID
			newData["transactionID"] = transactionID
			newData = json.dumps(newData)
			for i in range(1,len(ServerSockets)):
				helper.sendProposal(newData, ServerSockets[i])
	
		if(data["command"] == "ack"):
			print "received ack"
			transactionID = str(data["transactionID"][0]) + str(data["transactionID"][1])
			ACK_COUNT[transactionID]+=1
			if(ACK_COUNT[transactionID] >= MAJORITY):
				print "sending commit"
				helper.sendCommit(data["transactionID"], ServerSockets)





#Update with own log

#create sockets for all communications 

if (LEADER == MY_ID):
	thread.start_new_thread(createServerSockets, ())
	time.sleep(1)
(IP,PORT) = helper.getIPandPort(LEADER)
ClientCon = helper_connect.connectTo(IP,PORT)
helper.setProgramState("ready")
if (LEADER == MY_ID):
	for i in range(0,TOTAL_NODES):
		thread.start_new_thread(listenServerOnNetwork,(i+1,))
try:
	thread.start_new_thread(listenOnCLI, ())
	thread.start_new_thread(listenOnNetwork, ())
except:
	print "Thread start error"
while 1:
   time.sleep(0.5)