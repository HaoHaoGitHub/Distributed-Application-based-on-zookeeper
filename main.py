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
		thread.start_new_thread(listenServerOnNetwork,(i+1,))


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