import json
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
	return 3 #change to leader ID
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
	ServerSocket.send(data)
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
	ServerSocket.send(data)

def handleCliAppend(fileName,fileContents, ServerSocket, sender):
	data = {}
	data["command"] = "append"
	data["sender"] = sender
	data["fileName"] = fileName
	data["fileContents"] = fileContents
	data = json.dumps(data)
	ServerSocket.send(data)

def handleCliDelete(fileName,ServerSocket, sender):
	data = {}
	data["command"] = "delete"
	data["sender"] = sender
	data["fileName"] = fileName
	data = json.dumps(data)
	ServerSocket.send(data)



def sendProposal(newDat, ServerSockets):
	try:
		ServerSockets.send(newDat)
	except:
		print "counld not send proposal, please try later"

def writeLog(data):
	file = open("log.txt","a")
	try:
		data = json.loads(data)
	except:
		return
	try:
		data["originalRequest"]["fileContents"]
	except:
		data["originalRequest"]["fileContents"] = ""
	logEntry = str(data["transactionID"][0]) + "," + str(data["transactionID"][1]) + "," + data["originalRequest"]["command"] + "," + data["originalRequest"]["fileName"] + "," + data["originalRequest"]["fileContents"] + "\n"
	file.write(logEntry)
	file.close()

def replyAck(data,ServerSocket, sender):
	newData = {}
	newData["command"] = "ack"
	newData["sender"] = sender
	newData["transactionID"] = data["transactionID"]
	newData = json.dumps(newData)
	ServerSocket.send(newData)

def sendCommit(tID, ServerSockets):
	data = {}
	data["transactionID"] = tID
	data["command"] = "commit"
	data = json.dumps(data)
	for i in range(1,len(ServerSockets)):
		try:
			ServerSockets[i].send(data)
		except:
			print "could not send commit to a node"

# def executeOP(operation, FILES):
# 	command = operation["command"]
# 	fileName = operation ["fileName"]
# 	value = operation ["fileContents"]
# 	if (command == "create"):
# 		FILES[fileName] = value
# 	if(command == "append"):
# 		FILES[fileName] = FILES[fileName] + value
# 	if (command == "delete"):
# 		del FILES[fileName]

def sendSync(ServerSockets):
	f = open("log.txt")
	f = f.read()
	data = {}
	data ["command"] = "synchronise"
	data ["log"] = f
	data = json.dumps(data)
	for i in range(1,len(ServerSockets)):
		try:
			ServerSockets[i].send(data)
		except:
			print "This node not synced"

def executeOP(operation , FILES):
	logEntry = operation.split(",")
	command = logEntry[2]
	fileName = logEntry[3]
	value = logEntry[4]
	if (command == "create"):
		FILES[fileName] = value
	if(command == "append"):
		FILES[fileName] = FILES[fileName] + value
	if (command == "delete"):
		del FILES[fileName]
	return FILES