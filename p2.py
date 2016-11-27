import time, sys, socket, os, threading
from random import randint
from datetime import datetime

# global variables
host = ''                 # varibale that holds the IP address
is_leader = 0             # variable indicate whether the current process is the leader
leader = 0                # variable keep track of the leader process
holdingElection = False   # variable indicate whether is holding election

MY_ID = ''                # MY NODE ID 
MY_PORT = ''              # MY PORT NUMBER 
id_port = {}              # hashtable that holds {id, port number}
# end of global variables



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
# To initiate election, send ELECTION to all processes with higher IDs         
def new_election():
    global port
    global holdingElection
    global is_leader
    global id_port

    holdingElection = True;

    candidates = []
    for i in id_port:
        if i > MY_ID:
            candidates.append(i)

    NO_RESPONSE = True
    
    for c in candidates:
        ns = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            ns.connect((host, id_port[c]))
            ns.send('ELECTION')
            msg = ns.recv(2048)
            if msg == 'OK':
                print('OK received')
                NO_RESPONSE = False
            ns.close()
        except socket.error as e:
            print('Fail to communicate election')
            
    if NO_RESPONSE == True:
        is_leader = 1
        leader = MY_ID
        holdingElection = False
        # if no response received, send COORDINATOR to all processes with lower IDs
        send_coordinator()  
        
#-------------------------------------------------------------- 
# send COORDINATOR to all processes with lower IDs     
def send_coordinator():
    global port
    global holdingElection
    print('I am the leader')

    for i in id_port:
        if i < MY_ID:
            ns = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                ns.connect((host, id_port[i]))
                ns.send('COORDINATOR {}'.format(MY_ID))
                ns.close()
            except socket.error as e:
                print('Failed to send COORDINATOR')

#--------------------------------------------------------------
def answer_socket(conn, addr):
    msg = conn.recv(2048)
    analyze_msg(conn, addr, msg)
                           
#--------------------------------------------------------------  
# Function that verify and analyze the received messages  
def analyze_msg(conn, addr, msg):
    global holdingElection
    global is_leader
    global leader
    global holdingElection
    if msg == 'try':
        if is_leader == 1:
            print('alive')
            conn.send('alive')
    elif msg == 'alive':
        print('leader is alive')
    elif 'Are you the leader?' in msg:
        msplit = msg.split()
        asker = msplit[1]
        if is_leader == 1:
            if asker > MY_ID and holdingElection == False:
                is_leader = 0;
                leader = asker;
                new_election();
                conn.send('You are bigger than me')
            else:
                conn.send('Yes')
        else:
            conn.send('NO'.encode())
    elif msg == 'ELECTION':
        conn.send('OK')
        if holdingElection == False:
            new_election()
    elif 'COORDINATOR' in msg:
        # print(msg)
        msplit = msg.split()
        is_leader = 0
        leader = int(msplit[1])
        print 'The leader is ', leader
        if MY_ID > leader and holdingElection == False:
            new_election()
        else:
            holdingElection = False
    else:
        pass


#--------------------------------------------------------------
def main():
    global is_leader
    global DIR
    global port
    global leader
    global MY_NODE_NAME
    global MY_ID
    global id_port

    try:
        MY_ID = int(raw_input("What is my node name?\n"))
        f = open("ips.txt")

        for ips in f:
            ips = ips.split()
            id_port[int(ips[0])] = int(ips[2])
            if MY_ID == int(ips[0]):
                MY_PORT = int(ips[2])

        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        try:
            s.bind((host, MY_PORT))
        except socket.error as e:
            print(str(e))

        if is_leader == 0:
            for p in id_port:
                try:
                    check_leader(p)
                except:
                    print('connection failed')

        # If no leader found or no other processes is alive, 
        # then I am the leader
        if leader == 0:
            is_leader = 1;
            leader = MY_ID
            print('I\'m the leader.')

        s.listen(5)

        thread_check_leader = threading.Thread(target=check_leader_alive, args=())
        thread_check_leader.daemon = True
        thread_check_leader.start()


        while True:
            conn, addr = s.accept()
            new_thread = threading.Thread(target=answer_socket, args=(conn, addr))
            new_thread.daemon = True
            new_thread.start()

    except KeyboardInterrupt:
        sys.exit()


if __name__ == "__main__":
    main()  
#--------------------------------------------------------------


