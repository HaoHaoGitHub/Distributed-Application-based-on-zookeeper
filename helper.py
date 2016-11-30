import json
def announceCommit(clients,tid):
	msg = "SUCCESS :" + tid
	for i in range (0,len(clients)):
		clients[i].send(msg)

def announceFailure(clients,tid):
	msg = "FAILURE :" + tid
	for i in range(0,len(clients)):
		clients[i].send(msg)

def getIPandPort(nodeid):
	file = open("IPS.txt")
	for line in file:
		row = line.split()
		if (int(row[0]) == nodeid):
			return (row[1],int(row[2]))

def getID(ip):
	file = open("IPS.txt")
	for line in file:
		row = line.split()
		if(row[1] == ip):
			return int(row[0])

def findLeader():
	#hold leader election
	return 5 #change to leader ID
def setProgramState(state):
	file = open("state.txt")
	line = file.readlines()
	line[0] = state+"\n"
	file = open("state.txt","w")
	file.writelines(line)
	file.close()
def getProgramState():
	file = open("state.txt")
	line = file.readlines()
	file.close()
	return (line[0].rstrip())

def setTransactionNumber(tno):
	file = open("state.txt")
	line = file.readlines()
	line[1] = str(tno) + "\n"
	file = open("state.txt","w")
	file.writelines(line)
	file.close()


def getTransactionNumber():
	file = open("state.txt")
	line = file.readlines()
	file.close()
	return (line[1].rstrip())

def handleCliRead(fileName, FILES,ServerSocket, sender):
	data = {}
	data["command"] = "read"
	data["sender"] = sender
	data = json.dumps(data)
	data = data + "|"
	try:
		ServerSocket.send(data)
	except:
		print "Server DEAD"
	try:
		return FILES[fileName]
	except:
		return "Error: File not found"
def handleCliCreate(fileName,fileContents, ServerSocket, sender):
	data = {}
	data["command"] = "create"
	data["sender"] = sender
	data["fileName"] = fileName
	data["fileContents"] = fileContents
	data = json.dumps(data)
	data = data + "|"
	print "sending to leader"
	try:
		ServerSocket.send(data)
	except:
		print "server dead"

def handleCliAppend(fileName,fileContents, ServerSocket, sender):
	# try:
	# 	FILES[fileName]
	# except:
	# 	print ("Error: file does not exist")
	# 	return
	data = {}
	data["command"] = "append"
	data["sender"] = sender
	data["fileName"] = fileName
	data["fileContents"] = fileContents
	data = json.dumps(data)
	data = data + "|"
	print "sending to leader"
	try:
		ServerSocket.send(data)
	except:
		print "SERVER DEAD"

def handleCliDelete(fileName,ServerSocket, sender):
	data = {}
	data["command"] = "delete"
	data["sender"] = sender
	data["fileName"] = fileName
	data = json.dumps(data)
	data = data + "|"
	print "sending to leader"
	try:
		ServerSocket.send(data)
	except:
		print "SERVER DEAD"



def sendProposal(newDat, ServerSockets):

	newDat = newDat.split("|")
	newDat = json.loads(newDat[0])
	try:
		ServerSockets.send(json.dumps(newDat))
	except:
		pass
	# try:
	# 	ServerSockets.send(newDat)
	# except:
	# 	print "could not send proposal"
	# 	return "failed," + str(newDat["transactionID"][0]) + str(newDat["transactionID"][1])
	return "success,"+ str(newDat["transactionID"][0]) + str(newDat["transactionID"][1])



def writeLog(data):
	file = open("log.txt","a")
	# try:
	# 	data = json.loads(data)
	# except:
	# 	print "No new data to write to log"
	# 	return
	# try:
	# 	logEntry = str(data[0]) + "," + str(data[1]) + "," + data[2] + "," + data[3] + "," + data[4] + "\n"
	# except:
	# 	logEntry = str(data[0]) + "," + str(data[1]) + "," + data[2] + "," + data[3] + "," + "\n"
	data = data + "\n"
	file.write(data)
	file.close()
def eraseOwnLog():
	file = open("log.txt","w").close()
def writeSyncLog(data):
	file = open("log.txt","a")
	logEntry = data
	file.write(logEntry + "\n")
	file.close()
def replyAck(data,ServerSocket, sender):
	newData = {}
	newData["command"] = "ack"
	newData["sender"] = sender
	newData["transactionID"] = data["transactionID"]
	newData = json.dumps(newData)
	newData = newData + "|"
	try:
		ServerSocket.send(newData)
	except:
		print "SERVER DEAD"

def sendCommit(tID, ServerSockets):
	data = {}
	data["transactionID"] = tID
	data["command"] = "commit"
	data = json.dumps(data)
	data = data + "|"
	for i in range(0,len(ServerSockets)):
		try:
			ServerSockets[i].send(data)
		except:
			print "could not send commit to a node"

def sendSync(ServerSockets):
	f = open("log.txt")
	f = f.read()
	data = {}
	data ["command"] = "synchronise"
	data ["log"] = f
	data = json.dumps(data)
	data = data + "|"
	try:
		ServerSockets.send(data)
	except:
		print "One node not synced because it is down"

def executeOP(operation , FILES):
	logEntry = operation.split(",")
	command = logEntry[2]
	fileName = logEntry[3]
	value = ""
	try:
		value = logEntry[4]
	except:
		pass

	if (command == "create"):
		FILES[fileName] = value
		return FILES

	try:
		FILES[fileName]
	except:
		"Error: File not found"
		return FILES

	if(command == "append"):
		FILES[fileName] = FILES[fileName] + value
		return FILES

	if (command == "delete"):
		try:
			del FILES[fileName]
		except:
			print "FILE does not exist"
		return FILES

def sendFail(ServerSocket, Tid):
	data = {}
	data["command"] = "fail"
	data["transactionID"] = Tid
	data = json.dumps(data)
	data = data + "|"
	for i in range(0, len(ServerSocket)):
		try:
			ServerSocket[i].send(data)
		except:
			print "failed to send failure to a node"
# def finishPendingDelivery(WAIT_FOR):
# 	global FILES
# 	f = open("log.txt")
# 	for line in f:
# 		ogline = line
# 		line = line.split(",")
# 		tID = str(line[0]) + str(line[1])
# 		try:
# 			WAIT_FOR.remove(tID)
# 			executeOP(ogline,FILES)
# 		except:
# 			pass

