import time, sys, socket, os, threading
from random import randint
from datetime import datetime

DIR = '/tmp/bully/'       # a tmp directory hold all port numbers (not sure whether we 
                          # can do like this way in EC2 environment)
host = ''
port = int(sys.argv[1])   # port number from command argument
is_leader = 0             # variable indicate whether the current process is the leader
leader = 0                # variable keep track of the leader process
holdingElection = False   # variable indicate whether is holding election

#--------------------------------------------------------------
# function used for checking whether there is existing leader 
# in all processes, if the current process is larger than the exiting
# leader, initiate a new election
def check_leader(p):
    global leader
    ns = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    ns.settimeout(1)
    try:
        ns.connect((host, int(p)))
        ns.send('Are you the leader? {}'.format(port))
        msg = ns.recv(2048)
        if msg == 'YES':
            leader = int(p)
        elif msg == 'You are bigger than me':
            leader = port
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
            ns.settimeout(1)
            try:
                ns.connect((host, leader))
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
    global DIR
    global is_leader

    holdingElection = True;
    plist = os.listdir(DIR)
    candidates=[]
    for p in plist:
        if int(p) > port:
            candidates.append(int(p))
    NO_RESPONSE = True
    
    for c in candidates:
        ns = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            ns.connect((host, c))
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
        leader = port
        holdingElection = False
        # if no response received, send COORDINATOR to all processes with lower IDs
        send_coordinator()  
        
#-------------------------------------------------------------- 
# send COORDINATOR to all processes with lower IDs     
def send_coordinator():
    global port
    global holdingElection
    global DIR
    print('I am the leader')
    plist = os.listdir(DIR)

    for p in plist:
        if int(p) < port:
            ns = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                ns.connect((host, int(p)))
                ns.send('COORDINATOR {}'.format(port))
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
            conn.send('alive'.encode())
    elif msg == 'alive':
        print('leader is alive')
    elif 'Are you the leader?' in msg:
        # print(msg)
        msplit = msg.split()
        asker = msplit[1]
        if is_leader == 1:
            if asker > port and holdingElection == False:
                is_leader = 0;
                leader = asker;
                new_election();
                conn.send('You are bigger than me'.encode())
            else:
                conn.send('Yes'.encode())
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
        print 'The leader is', leader
        if port > leader and holdingElection == False:
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
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        try:
            s.bind((host, port))
        except socket.error as e:
            print(str(e))

        try:
            os.chdir(DIR)
        except:
            os.mkdir(DIR)

        plist = os.listdir(DIR)

        for i in plist:
            print i

        if not plist:
            is_leader = 1
            leader = port
            print('I\'m the leader.')

        mySock = sys.argv[1]
        f = open(mySock, 'w+')

        if is_leader == 0:
            for p in plist:
                try:
                    check_leader(p)
                except:
                    print('connection failed')


        s.listen(5)

        thread_check_leader = threading.Thread(target=check_leader_alive, args=())
        thread_check_leader.daemon = True
        thread_check_leader.start()
        # wait(1)
        # if is_leader == 1:
        #     print('The leader is: {}'.format(leader))
        while True:
            conn, addr = s.accept()
            new_thread = threading.Thread(target=answer_socket, args=(conn, addr))
            new_thread.daemon = True
            new_thread.start()

    except KeyboardInterrupt:
        os.remove(sys.argv[1])
        sys.exit()

    except:
        print('Remove File')
        os.remove(sys.argv[1])

if __name__ == "__main__":
    main()  
#--------------------------------------------------------------


