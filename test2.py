import socket,thread,time,pickle,copy

clientSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
breakDown = False
IP = raw_input("Enter server IP \n")
try:
	clientSocket.connect((IP,8082))
except:
	print "server down"
	breakDown = True
def listenCli():
	global breakDown
	while 1 and not breakDown:
		command = raw_input("Enter command,filename,value")
		if (command == "exit"):
			breakDown = True
			break
		clientSocket.send(command)
		time.sleep(2)
def listenNW():
	global breakDown
	while 1 and not breakDown:
		print clientSocket.recv(512)
thread.start_new_thread(listenCli,())
thread.start_new_thread(listenNW,())
while not breakDown:
	time.sleep(2)
