import sys
import socket
import message
import json
import math
import random
import threading
import queue
import string

debug = False

class Peer:
    class Blocker():
        def __init__(self):
            self.blocker = threading.Event()
            self.response_data = queue.Queue()

    def __init__(self, peer_name, mport, pport) -> None:
        self.peer_name = peer_name
        self.ipv4_address = '127.0.0.1'#socket.gethostbyname(socket.gethostname())
        #self.ipv4_address = '10.120.70.117'
        self.mport = mport
        self.pport = pport

        self.manager_ipv4_address_and_port = (sys.argv[1], int(sys.argv[2]))
        self.ring_size = None
        self.identifier = None

        self.local_dht = {}
        self.database_year = None

        #right neighbor used for management message passing protocol 
        self.right_neighbor_info = None

        #list of peers and their addresses used for hot potato protocol
        self.peer_addresses = {}

        self.socket_to_manager = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket_to_manager.bind((self.ipv4_address, self.mport))

        self.socket_to_peers = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket_to_peers.bind((self.ipv4_address, self.pport))

        #to be able to simultaneously execute user commands and requests from other peers
        self.listen_to_peers_thread = threading.Thread(target=self.receive_from_peers)
        self.listen_to_peers_thread.start()

        self.backup_thread = threading.Thread(target=self.receive_from_peers)
        self.backup_thread.start()

        self.query_dht_blocker = self.Blocker()
        self.teardown_blocker = self.Blocker()
        self.reset_id_blocker = self.Blocker()
        self.rebuild_dht_blocker = self.Blocker()
        self.join_dht_blocker = self.Blocker()

    def register(self):

        self.send_to_manager("register", None)
        response = self.receive_from_manager()
        print(response[0], response[1])

    def setup_dht(self, n, year, ring_established=False):
        
        parameters = {}
        parameters["n"] = n
        parameters["year"] = year
        
        self.send_to_manager("setup-dht", parameters)
        response = self.receive_from_manager()

        if not response[0]=="SUCCESS":
            print(response[0], response[1])
        else:
            print("Manager has returned a list of peers in response to setup-dht")
            peer_list = response[1][0]

            self.identifiers_and_neighbours(peer_list, response[1][1])
            self.constructing_dht(year)
            self.dht_complete()        
        return response

    def identifiers_and_neighbours(self, three_tuple_peer_list, year):

        if debug:
            print("three tuple peerlist: ", three_tuple_peer_list)
            print("dht at this moment: ", self.local_dht)

        self.ring_size = len(three_tuple_peer_list)

        #####
        #prob should implement threading for each peer so they can continue to receieve messages and execute them 
        #simultaneously instead of blocking and waiting to process future commands until finished. this would allow
        #the leader to send this message below to itself in the for loop instead of having to have a seperate a call
        #####
        #set_id_msg = message.PeertoPeerMessage(command="set-id", identifier=0, ring_size=self.ring_size, additional_parameters=True)
        
        #leader calls this for itself
        #self.set_id(msg=set_id_msg, additional_params=[three_tuple_peer_list, year])
        
        if debug:
            print("this is our neighbor: ", self.right_neighbor_info)

        for identifier in range(len(three_tuple_peer_list)):
            
            command_parameters = {}
            command_parameters["identifier"] = identifier
            params = [three_tuple_peer_list, year]
            print("three tuple peer list: ", three_tuple_peer_list)
            print(f"trying to send to peer address: {tuple(three_tuple_peer_list[identifier][1])}")
            self.send_message_to_peer("set-id", command_parameters, params, tuple(three_tuple_peer_list[identifier][1]))

    def calculate_s(self, year):
        filename = f"details-{year}.csv"
        with open(filename, 'r') as file:
            #skip first line of field names
            next(file)
            total_records = 0

            datafile_lines = file.readlines()
        
            s = 2*len(datafile_lines) + 1
        
            while True:
                found = self.test_for_prime(s)
                if found:
                    break
                else:
                    s += 1
        return s        
    
    def constructing_dht(self, year):
       
        filename = f"details-{year}.csv"
        with open(filename, 'r') as file:
            #skip first line of field names
            next(file)
            total_records = 0

            datafile_lines = file.readlines()
        
            s = self.calculate_s(self.database_year)
            numrecords_perpeer = {}

            print("next biggest prime is: ", s)
            print(f"amount of records in hash table is {len(datafile_lines)}")

            with open("store_log.txt", "w") as f:
                
                f.close()

            for line in datafile_lines:
                
                record = line.strip().split(',')
                event_id, _, _, _, _, _, _, _, _, _, _, _, _, _ = record
                identifier_storedat = self.storing(int(event_id), record, s)
                
                if debug:
                    if identifier_storedat in numrecords_perpeer:
                        numrecords_perpeer[identifier_storedat] += 1
                    else:
                        numrecords_perpeer[identifier_storedat] = 1

            if debug:
                for identifier in sorted(numrecords_perpeer.keys()):

                    if numrecords_perpeer[identifier] > 1:
                        print(f"Node {identifier} has stored {numrecords_perpeer[identifier]} records")
            
                print("num of pos of record entires: ", len(numrecords_perpeer))
                



    
    def storing(self, event_id, record, s):
        
        pos_of_record = event_id % s

        id = pos_of_record % self.ring_size

        if debug:
            print("event id is: ", event_id)
            print(f"s is: {s}")
            print(f"pos of record is : {pos_of_record}")
            print(f"ring size is : {self.ring_size}")

            print(f"id is: {id}")
        
        command_parameters = {}
        command_parameters["identifier"] = id
        command_parameters["pos_of_record"] = pos_of_record
        command_parameters["event_id"] = event_id
        
        if id == self.identifier:
            self.send_message_to_peer("store", command_parameters, record, (self.ipv4_address, self.pport))
        else:
            self.send_message_to_peer("store", command_parameters, record, self.right_neighbor_info)
        
        if debug:
            with open("store_log.txt", "a") as f:
                f.write(f'Node {id}: {record}\n')
                f.close()
        
        return pos_of_record


    def test_for_prime(self, val):

        limit = int(math.sqrt(val) + 0.5)
        factor = 2
        
        while factor <= limit and val % factor != 0:
            factor += 1
        
        return factor > limit

    def dht_complete(self):

        self.send_to_manager("dht-complete", None)
        response = self.receive_from_manager()
        print(response[0], response[1])

    #initialize right neighbor in ring topology and list of all peer addresses in DHT
    def set_id(self, msg, additional_params):


        
        self.identifier = msg.identifier
        self.ring_size = msg.ring_size
        
        peer_list = 0
        neighbor_node = (msg.identifier + 1) % msg.ring_size
        neighbor_address = 1

        year = 1


        self.right_neighbor_info = tuple(additional_params[peer_list][neighbor_node][neighbor_address])
        self.database_year = additional_params[year]

        peer_name = 0
        peer_address = 1
        self.peer_addresses.clear()
        for peer_identifier in range(len(additional_params[peer_list])):
            self.peer_addresses[peer_identifier]=additional_params[peer_list][peer_identifier][peer_address]

    def store(self, msg, record):
        
        if msg.identifier==self.identifier:
            if msg.pos_of_record in self.local_dht:
                # print("before adding")
                # print(self.local_dht[msg.pos_of_record])
                self.local_dht[msg.pos_of_record][msg.event_id] = record
                # print("added a duplicate safely")
                # print(self.local_dht[msg.pos_of_record])
            
            self.local_dht[msg.pos_of_record] = {msg.event_id : record}        
          
        else:
            self.send_message_to_peer("reuse-message", msg, record, self.right_neighbor_info)
            
    def query_dht(self, event_id):
        self.send_to_manager("query-dht", None)
        manager_response = self.receive_from_manager()
        if not manager_response[0]=="SUCCESS":
            print(f"{manager_response[0]}: {manager_response[1]}")
            return

        else:
            command_params = {}
            command_params["event_id"] = event_id
            params=[(self.peer_name, self.ipv4_address, self.pport), []]
            # print(f"manager response is {manager_response}")
            next_peer_address = (manager_response[1][1], int(manager_response[1][2]))
            print(f"Trying to find the record starting at peer {manager_response[1][0]} with address: {next_peer_address}")
            self.send_message_to_peer("find-event",command_params,params,next_peer_address)
            self.query_dht_blocker.blocker.wait()
            response = self.query_dht_blocker.response_data.get()
            result = 0
            if response[result]=="SUCCESS":
                record = 2
                visited_nodes = 3
                # print(response[visited_nodes])
                database_year = 4
                with open(f"details-{response[database_year]}.csv") as f:
                    categories = f.readline().strip('\n').split(',')
                    # print(categories)
                # print("response record is: ", response[record])
                for i in range(len(response[record])):
                    print(f"{categories[i]}: {response[record][i]}")

                print(f"The sequence of nodes visited to find the record is: {' '.join([str(num) for num in response[visited_nodes]])}")
            else:
                reason = 2
                print(response[result], response[reason])

    def deregister(self):
        self.send_to_manager("deregister", None)
        response = self.receive_from_manager()
        print(response[0], response[1])
        if response[0]=="SUCCESS":
            try:
                self.socket_to_manager.close()
                self.socket_to_peers.close()
            except Exception as E:
                pass
            sys.exit(0)

    #additional_params has format of: [original sending peer's (S) 3-tuple, non-visited set of nodes, id_seq]
    def find_event(self, msg, additional_params):

        print(f"Determining if record with event id {msg.event_id} is stored by {self.peer_name}")
        id_seq = 1
        original_sender_info = 0

        pos_of_record = msg.event_id % self.calculate_s(self.database_year)
        identifier_storing_record = pos_of_record % self.ring_size

        id_seq = additional_params[id_seq]

        id_seq.append(self.identifier)

        if identifier_storing_record==self.identifier and pos_of_record in self.local_dht and msg.event_id in self.local_dht[pos_of_record]:
                    result = "SUCCESS"
                    original_command = "find-event"
                    params = [result, original_command, self.local_dht[pos_of_record][msg.event_id], id_seq, self.database_year]
                    starting_sender_peer_address = (additional_params[original_sender_info][1], int(additional_params[original_sender_info][2]))
                    self.send_response_to_peer(parameters=params, receiver_address=starting_sender_peer_address)
            
        else:
            nonvisited_set = [identifier for identifier in self.peer_addresses.keys() if identifier not in id_seq]
            print(f"Nodes visited so far (including this one): {id_seq}")
            print(f"Nodes not visited yet: {nonvisited_set}")
            all_visited = False

            try:
                next_peer = random.choice(list(nonvisited_set))
            except Exception as E:
                all_visited = True

            if all_visited:
                result = "FAILURE"
                original_command = "find-event"
                reason = f"Storm event {msg.event_id} not found in the DHT"
                params = [result, original_command, reason,]
                starting_sender_peer_address = (additional_params[original_sender_info][1], int(additional_params[original_sender_info][2]))
                self.send_response_to_peer(parameters=params, receiver_address=starting_sender_peer_address)
            else:
                print("Sending to next peer: ", next_peer)
                params = [additional_params[original_sender_info], id_seq]
                self.send_message_to_peer("reuse-message", msg, params, tuple(self.peer_addresses[next_peer]))
    
    def join_dht(self):
        
        self.send_to_manager("join-dht", None)
        manager_response = self.receive_from_manager()
        print(manager_response)
        print(f"{manager_response[0]}: {manager_response[1][0]}")
        if not manager_response[0]=="SUCCESS":
            return
        else:
            curr_dht_leader= tuple(manager_response[1][1])
            params = [self.ipv4_address, self.pport]
            print(f"Asking leader of the current DHT ({curr_dht_leader}) to tear it down")
            self.send_message_to_peer("teardown-dht", None, params, curr_dht_leader)
            self.join_dht_blocker.blocker.wait()
            response = self.join_dht_blocker.response_data.get()
            
            print(f"Setting up a new DHT as the leader")
            self.setup_dht(response[3] + 1, response[4])
            self.send_to_manager("dht-rebuilt", None)
            manager_response = self.receive_from_manager()
            print(f"{manager_response[0]}: {manager_response[1]}")
            
    def leave_dht(self):
        self.send_to_manager("leave-dht", None)
        manager_response = self.receive_from_manager()
        print(f"{manager_response[0]}: {manager_response[1]}")
        if not manager_response[0]=="SUCCESS":
            return
        else:
            command_params = {}
            command_params["identifier"] = self.identifier
            self.send_message_to_peer("teardown", command_params, None, self.right_neighbor_info)
            self.teardown_blocker.blocker.wait()



            command_params = {}
            command_params["identifier"] = 0
            command_params["ring_size"] = self.ring_size - 1

            params = self.identifier
            self.send_message_to_peer("reset-id", command_params, params, self.right_neighbor_info)
            self.reset_id_blocker.blocker.wait()

            params = [self.database_year, [self.ipv4_address, self.pport]]
            self.send_message_to_peer("rebuild-dht", None, params, self.right_neighbor_info)
            
            self.rebuild_dht_blocker.blocker.wait()
            response = self.rebuild_dht_blocker.response_data.get()
            
            result = 0
            if response[result]=="SUCCESS":
                self.send_to_manager("dht-rebuilt", None)
                manager_response = self.receive_from_manager()
                print(f"{manager_response[0]}: {manager_response[1]}")
            else:
                print(f"{response[result]}: {response[2]}")
            

   
    def reset_id(self, msg, additional_params):
        
        if not self.identifier==additional_params:
            print(f"Reset identifier of original node {self.identifier} to {msg.identifier}")
            self.identifier = msg.identifier
            self.ring_size = msg.ring_size
            command_params = {}
            command_params["identifier"] = (self.identifier + 1) % self.ring_size
            command_params["ring_size"] = self.ring_size
            self.send_message_to_peer("reset-id", command_params, additional_params, self.right_neighbor_info)
        else:
            self.reset_id_blocker.blocker.set()
    
    def teardown(self, msg):
        print(f"Local DHT of node {self.identifier} has been deleted")
        self.local_dht = {}
        if not self.identifier==msg.identifier:
            print(f"Sending teardown command to neighbor at {self.right_neighbor_info}")
            self.send_message_to_peer("reuse-message", msg, None, self.right_neighbor_info)
        else:
            print(f"Starting node has been reached; every node has torn down their local DHT")
            self.teardown_blocker.blocker.set()


    def teardown_dht(self):
        print("Seeking manager approval to teardown DHT from peer")
        self.send_to_manager("teardown-dht", None)
        manager_response = self.receive_from_manager()
        if not manager_response[0]=="SUCCESS":
            print(f"{manager_response[0]}: {manager_response[1]}")
            return
        else:
            command_params = {}
            command_params["identifier"] = self.identifier
            print(f"Sending teardown command to peers in the current ring topology, starting with peer at {self.right_neighbor_info}")
            self.send_message_to_peer("teardown", command_params, None, self.right_neighbor_info)
            self.teardown_blocker.blocker.wait()
            print("Teardown complete")
            self.send_to_manager("teardown-complete", None)
            manager_response = self.receive_from_manager()
            print(f"{manager_response[0]}: {manager_response[1]}")

    def send_to_manager(self, command, command_parameters):
        if command=="register":
            msg = message.PeertoManagerMessage(command="register", peer_name=self.peer_name, peer_ipv4_address=self.ipv4_address, peer_mport=self.mport, peer_pport=self.pport)
        elif command=="setup-dht":
            msg = message.PeertoManagerMessage(command="setup-dht", peer_name=self.peer_name, n=command_parameters["n"], year=command_parameters["year"])
        elif command=="dht-complete":
            msg = message.PeertoManagerMessage(command="dht-complete", peer_name=self.peer_name)
        elif command=="query-dht":
            msg = message.PeertoManagerMessage(command="query-dht", peer_name=self.peer_name)
        elif command=="leave-dht":
            msg = message.PeertoManagerMessage(command="leave-dht", peer_name=self.peer_name)
        elif command=="dht-rebuilt":
            msg = message.PeertoManagerMessage(command="dht-rebuilt", peer_name=self.peer_name)
        elif command=="teardown-dht":
            msg = message.PeertoManagerMessage(command="teardown-dht", peer_name=self.peer_name)
        elif command=="teardown-complete":
            msg = message.PeertoManagerMessage(command="teardown-complete", peer_name=self.peer_name)
        elif command=="join-dht":
            msg = message.PeertoManagerMessage(command="join-dht", peer_name=self.peer_name)
        elif command=="deregister":
            msg = message.PeertoManagerMessage(command="deregister", peer_name=self.peer_name)
        else:
            pass

        msg_in_byteform = msg.pack_message()
        self.socket_to_manager.sendto(msg_in_byteform, self.manager_ipv4_address_and_port)
    
    def send_message_to_peer(self, command, command_parameters, parameters, receiver_address):
        if command=="set-id":
            msg = message.PeertoPeerMessage(command="set-id", identifier=command_parameters["identifier"], ring_size=self.ring_size, event_id=0, additional_parameters=True)
        elif command=="store":
            msg = message.PeertoPeerMessage(command="store", identifier=command_parameters["identifier"], pos_of_record=command_parameters["pos_of_record"], event_id=command_parameters["event_id"], additional_parameters=True)
        elif command=="find-event":
            msg = message.PeertoPeerMessage(command="find-event", event_id=command_parameters["event_id"], additional_parameters=True)
        elif command=="reuse-message":
            #command_parameters stores a message object in this case
            msg = command_parameters
        elif command=="teardown":
            msg = message.PeertoPeerMessage(command="teardown", identifier=command_parameters["identifier"], additional_parameters=False)
        elif command=="reset-id":
            msg = message.PeertoPeerMessage(command="reset-id", identifier=command_parameters["identifier"], ring_size=command_parameters["ring_size"],additional_parameters=True)
        elif command=="rebuild-dht":
            msg = message.PeertoPeerMessage(command="rebuild-dht", ring_size=self.ring_size-1,additional_parameters=True)
        elif command=="teardown-dht":
            msg = message.PeertoPeerMessage(command="teardown-dht",additional_parameters=True)
        else:
            pass

        msg_in_byteform = msg.pack_message()
        parameters = json.dumps(parameters).encode('utf-8')

        full_message = msg_in_byteform + parameters + message.message_delimiter
        self.socket_to_peers.sendto(full_message, receiver_address)

    def send_response_to_peer(self, parameters, receiver_address):
        
        msg_byteform = message.Response(parameters=parameters).build_response()
        self.socket_to_peers.sendto(msg_byteform, receiver_address)

    
    def retrieve_info(self):
        print(f"Peer name is {self.peer_name}")
        print(f"IPv4: {self.ipv4_address} Manager Port: {self.mport} Peer Port: {self.pport}")
        print(f"Identifier is: {self.identifier}")
        print(f"Right neighbor address is: {self.right_neighbor_info}")
        print(f"Local DHT size is: {len(self.local_dht)}")
        print(f"Year of database is: {self.database_year}")
        print(f"Peer database is:\n{self.peer_addresses}")
        print(f"DHT contents:\n{self.local_dht}")
        
            

    def handle_peer_request(self, msg, additional_params):
        print(f"Received command request {msg.command}")
        if msg.command=="set-id":
            self.set_id(msg, additional_params)
        elif msg.command=="store":
            self.store(msg, additional_params)
        elif msg.command=="find-event":
            self.find_event(msg, additional_params)
        elif msg.command=="teardown":
            self.teardown(msg)
        elif msg.command=="reset-id":
            self.reset_id(msg, additional_params)
        elif msg.command=="teardown-dht":
            print(additional_params)
            self.teardown_dht()
            result = "SUCCESS"
            original_command = "teardown-dht"
            reason = "DHT has been destroyed, you can now remake it"
            self.send_response_to_peer([result, original_command, reason, self.ring_size, self.database_year], tuple(additional_params))

        elif msg.command=="rebuild-dht":
            response = self.setup_dht(msg.ring_size, additional_params[0], ring_established=True)
            print("DHT has been rebuilt")
            result = "SUCCESS"
            original_command = "rebuild-dht"
            reason = "DHT has been rebuilt, you can tell the manager now"
            self.send_response_to_peer([result, original_command, reason], tuple(additional_params[1]))
            
        #add more cases as we progress on the project
    
    def receive_from_manager(self):
        msg_in_byteform = b''
        while message.message_delimiter not in msg_in_byteform:
            msg_in_byteform += self.socket_to_manager.recv(4096)
        
        return json.loads(msg_in_byteform.strip(message.message_delimiter))


    def receive_from_peers(self):
        
        while True:
            
            #print("Listenting on peer socket for further commands\n")
            
            msg_in_byteform = b''
            while message.message_delimiter not in msg_in_byteform:
                try:
                    msg_in_byteform += self.socket_to_peers.recv(4096)
                except Exception as E:
                    print("Thread says bye, MargaritaNet will miss you")
                    return
            
            peer_response = True
            try:
                parameters = json.loads(msg_in_byteform)
            except Exception as E:
                peer_response = False

            if not peer_response:
                msg = message.PeertoPeerMessage.unpack_message(msg_in_byteform[:message.PeertoPeerMessage.message_length])
            
                #print(f"received request: {msg.command}")
                if msg.additional_parameters==True: 
                    additional_parameters = msg_in_byteform[message.PeertoPeerMessage.message_length:].strip(message.message_delimiter)                
                    additional_parameters = json.loads(additional_parameters)
                
                    self.handle_peer_request(msg, additional_parameters)
                
                else:
                    response = self.handle_peer_request(msg, None)
            elif peer_response:
                #print("this is a peer response")
                result = 0
                original_command = 1
                if parameters[original_command]=="find-event":
                    self.query_dht_blocker.response_data.put(parameters)
                    self.query_dht_blocker.blocker.set()
                elif parameters[original_command]=="rebuild-dht":
                    self.rebuild_dht_blocker.response_data.put(parameters)
                    self.rebuild_dht_blocker.blocker.set()
                elif parameters[original_command]=="teardown-dht":
                    self.join_dht_blocker.response_data.put(parameters)
                    self.join_dht_blocker.blocker.set()

    def handle_user_request(self, command, additional_params):
            if command=="register":
                self.register()
            elif command=="setup-dht":
                self.setup_dht(n=additional_params[0], year=additional_params[1])
            elif command=="query-dht":
                self.query_dht(event_id=additional_params[0])
            elif command=="info":
                self.retrieve_info()
            elif command=="leave-dht":
                self.leave_dht()
            elif command=="join-dht":
                self.join_dht()
            elif command=="teardown-dht":
                self.teardown_dht()
            elif command=="deregister":
                self.deregister()


def find_nearest_open_ports():
    
    starting_port_num = 13002
    #print(socket.gethostname())
    name = '127.0.0.1'# socket.gethostbyname(socket.gethostname())
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    
    while True:
        try:
            #print(f"Address attempted is {(name, starting_port_num)}")
            sock.bind((name, starting_port_num))
            #print(res)
            sock.close()
            #print(f"Starting port for this peer is {starting_port_num}")
            return starting_port_num
        except Exception as E:
            starting_port_num += 2

if __name__=="__main__":
    peer = None
    valid_commands = ["register", "setup-dht", "query-dht", "leave-dht", "join-dht", "deregister", "teardown-dht", "help", "info"]
    print("Welcome to MargaritaNet! To see a list of valid commands, input \"help\" when prompted for a command.")
    while True:
        print("Input a command:")
        command = sys.stdin.readline().rstrip('\n')
    
        if not command in valid_commands:
            print("Invalid command. Valid commands are:")
            print(f"{' '.join(valid_commands)}")
            continue
        else:
            params = None
            #peer hasn't been created yet
            if peer == None:
                if not command=="register":
                    print("You must register before performing any other command.")
                    continue
                else:
                    print("Input the name you'd like to be known as below (only alphabetic and at most 15 characters):")
                    name = sys.stdin.readline().rstrip('\n')
                    if name.isalpha():
                        mport = find_nearest_open_ports()
                        peer = Peer(peer_name=name[:15], mport=mport, pport=(mport+1))
                        params = None
                    else:
                        print("Chosen name contains non-alphabetic characters. Please re-register with a new name.")
                        continue
            else:
                if command=="setup-dht":
                    print("Input desired size of the distributed hash table (must be > 3):")
                    size = int(sys.stdin.readline().rstrip('\n'))
                    print("Input desired year you'd like to store (from 1950-1999):")
                    year = sys.stdin.readline().rstrip('\n')
                    params = [size, year]
                elif command=="query-dht":
                    print("Input the event_id of the record you'd like to retrieve:")
                    event_id = int(sys.stdin.readline().rstrip('\n'))
                    params = [event_id]
                elif command=="deregister":
                    params = None

                else:
                    pass 

            peer.handle_user_request(command=command, additional_params=params)





        