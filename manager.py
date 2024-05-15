import socket
import sys
import message
import time
import random
import json

chosen_port = int(sys.argv[1])
chosen_IP = '127.0.0.1'
#chosen_IP = '10.120.70.154'

#entries will be in the format of: 
#peer_name: [(IP, mport, pport), state]
peer_database = {}
dht_leader = ""
dht_completed = False
followup_command_required = False
message_length = message.PeertoManagerMessage.message_length

#peer name: [command_waiting for, peer_that_must_be_sender]
waiting_for_followup = {}

debug = False

def register(received_message):

    if debug:
        print(f"\nPeer name is {received_message.peer_name}\n")
        print(f"Peer database looks like:\n{peer_database}")
        res = received_message.peer_name in peer_database
        print(f"is the peer already present in the network? {res}")


    for peer_name in peer_database:
        if received_message.peer_pport==peer_database[peer_name][0][2]:
            result= "FAILURE"
            reason = "This peer port already in use by another peer"
            params = reason

    if received_message.peer_name in peer_database:
        result = "FAILURE"
        reason = "Peer name is already registered"
        params = reason

    else:
        peer_database[received_message.peer_name] = [(received_message.peer_ipv4_address, received_message.peer_mport, received_message.peer_pport), "Free"]
        result = "SUCCESS"
        params = 'Registered with the manager!'
    

    response = build_response_to_peer(result, params)    
    return response

def setup_dht(received_message):

    global dht_completed

    if dht_completed:
        result = "FAILURE"
        reason = "DHT already completed"
        params = reason

    elif received_message.peer_name not in peer_database:
        #print(peer_database)
        #print(received_message.peer_name)
        result = "FAILURE"
        reason = "Peer name not registered"
        params = reason

    elif received_message.n < 3:
        result = "FAILURE"
        reason = "DHT size must be at least 3"
        params = reason

    elif len(peer_database) < received_message.n:
        result = "FAILURE"
        reason = "Not enough registered users"
        params = reason
    
    else:
        result = "SUCCESS"

        #peer list is a list and not a dict so we can preserve order of insertion
        peer_list = []

        #setting first peer as leader
        peer_database[received_message.peer_name][1] = "Leader"

        #n-1 free peers randomly selected
        
        peers_in_dht = False
        for peer in peer_database:
            if peer_database[peer][1] == "InDHT":
                peers_in_dht = True
                break
        
        if not peers_in_dht:
            print("No existing DHT. Setting up a fresh DHT")
            free_peers = [peer_name for peer_name in peer_database.keys() if peer_database[peer_name][1] == "Free"]
        else:
            print("There is an existing DHT. Redistributing records among available peers.")
            free_peers = [peer_name for peer_name in peer_database.keys() if peer_database[peer_name][1] == "InDHT"]
        # print("n is : ", received_message.n)
        # print("free peers is: ", len(free_peers))
        # print(free_peers)
        # print(received_message.n)
        # print("WHAT")
        listed_peers = random.sample(free_peers, received_message.n-1)
        
        peer_list.append((received_message.peer_name, (peer_database[received_message.peer_name][0][0], peer_database[received_message.peer_name][0][2])))
        
        for peer_name in listed_peers:
            
            peer_database[peer_name][1] = "InDHT"
            peer_list.append((peer_name, (peer_database[peer_name][0][0], peer_database[peer_name][0][2])))
        
        params = [peer_list, received_message.year]
        global dht_leader 
        dht_leader = received_message.peer_name

    print(f"Peer lsit being sent is\n\n{peer_list}")
    response = build_response_to_peer(result, params)
    return response

def dht_complete(received_message):

    if not peer_database[received_message.peer_name][1] == "Leader":
        result = "FAILURE"
        reason = f"Peer who sent completion ({received_message.peer_name}) is not the leader ({dht_leader})"
        params = reason
    else:
        global dht_completed
        dht_completed = True
        result = "SUCCESS"
        params = 'DHT completion has been approved by the manager!'
    
    response = build_response_to_peer(result, params)
    return response

def query_dht(received_message):

    print("QUERY")
    print(peer_database)

    result = "FAILURE"
    
    if not dht_completed:
        reason = "DHT setup has not been completed"
        params = reason
    elif received_message.peer_name not in peer_database:
        reason = f"FAILURE- peer {received_message.peer_name} is not registered with the network"
        params = reason
    elif not peer_database[received_message.peer_name][1] == "Free":
        reason = f"FAILURE- peer {received_message.peer_name} is not free"
        params = reason
    else:
        result = "SUCCESS"
        peers_in_network = [peer for peer in peer_database if peer_database[peer][1] == "InDHT"]
        peer_in_network = random.sample(population=peers_in_network, k=1)[0]
        # print(type(peer_in_network))
        params = (peer_in_network, peer_database[peer_in_network][0][0], peer_database[peer_in_network][0][2])
    
    response = build_response_to_peer(result, params)
    return response

def leave_dht(received_message):
    
    if peer_database[received_message.peer_name][1] == "Free":
        # print(peer_database)
        result = "FAILURE"
        reason = f"Peer who sent the leave DHT request ({received_message.peer_name}) is not in the DHT"
        params = reason
    else:
        result = "SUCCESS"
        params = 'Approval has been given by the manager to leave the DHT'

    waiting_for_followup[received_message.peer_name] = "dht-rebuilt"
    
    response = build_response_to_peer(result, params)
    
    global dht_completed
    dht_completed = False

    peer_database[received_message.peer_name][1] = "Free"
    for peer in peer_database:
        if peer_database[peer][1] == "Leader":
            peer_database[peer][1] = "InDHT"


    return response

def dht_rebuilt(received_message):
    
    #failure is handled in the wait_for_command func

    result = "SUCCESS"
    params = 'Manager has approved the rebuilding of the DHT'
    response = build_response_to_peer(result, params)
    return response

def teardown_dht(received_message):
    
    if not peer_database[received_message.peer_name][1] == "Leader":
        result = "FAILURE"
        reason = f"Peer who sent the teardown request ({received_message.peer_name}) is not the leader ({dht_leader})"
        params = reason
    else:
        result = "SUCCESS"
        params = 'DHT teardown approval has been given by the manager!'
    
    response = build_response_to_peer(result, params)
    return response

def teardown_complete(received_message):
    
    if not peer_database[received_message.peer_name][1] == "Leader":
        result = "FAILURE"
        reason = f"Peer who sent teardown completion approval ({received_message.peer_name}) is not the leader ({dht_leader})"
        params = reason
    else:
        result = "SUCCESS"
        global dht_completed
        dht_completed = False
        for peer in peer_database.keys():
            if peer_database[peer][1] == "InDHT" or peer_database[peer][1] == "Leader":
                peer_database[peer][1] = "Free"

        params = 'DHT teardown approval has been given by the manager. All peers are now free.'
    
    response = build_response_to_peer(result, params)
    return response

def deregister(received_message):
    #first check if peer has been registered
    if received_message.peer_name not in peer_database:
        result = "FAILURE"
        reason = f"Peer {received_message.peer_name} is not registered with the network"
        params = reason
    elif peer_database[received_message.peer_name][1] == "InDHT":
        result = "FAILURE"
        reason = f"Peer {received_message.peer_name} is InDHT"
        params = reason
    else: #received_message.peer_name in peer_database:
        result = "SUCCESS"
        reason = f"Peer {received_message.peer_name} has been deregistered"
        params = reason
        del peer_database[received_message.peer_name]

    response = build_response_to_peer(result, params)
    return response

def join_dht(received_message):
    
    global dht_completed
    
    if not peer_database[received_message.peer_name][1] == "Free":
        result = "FAILURE"
        reason = f"Peer who sent teardown completion approval ({received_message.peer_name}) is not Free"
        params = reason
    
    elif not dht_completed:
        result = "FAILURE"
        reason = f"There is no existing DHT to join"
        params = reason

    else:
        result = "SUCCESS"
        dht_completed = False
        params = 'Approval has been given by the manager to join the DHT.'
    
    #waiting_for_followup[received_message.peer_name] = "dht-rebuilt"

    response = build_response_to_peer(result, [params, [peer_database[dht_leader][0][0], peer_database[dht_leader][0][2]]])
    return response

def build_error_message(recv_message):
    required_command = 0
    result = "FAILURE"
    reason = f"Command received ({recv_message.command}) from peer {recv_message.peer_name} is not the required next command ({waiting_for_followup[recv_message.peer_name]}) from this peer"
    response = build_response_to_peer(result, reason)
    return response

def build_response_to_peer(result, params):

    response = message.Response([result, params]).build_response()
    return response

def followup_actions(peer_name, command):
    del waiting_for_followup[peer_name]
    # if command=="dht-rebuilt":
    #     print(f"Peer {peer_name} has left the DHT; its status is now set to Free")
    #     peer_database[peer_name] = "Free"
    # else:
    #     pass


def handle_request(request):
    recv_message = message.PeertoManagerMessage.unpack_message(request)
    command = recv_message.command

    if recv_message.peer_name in waiting_for_followup:
        if not command==waiting_for_followup[recv_message.peer_name]:
            response = build_error_message(recv_message)
            return response
        else:
            followup_actions(recv_message.peer_name, command)

    if command=="register":
        response = register(recv_message)
    elif command=="setup-dht":
        response = setup_dht(recv_message)
    elif command=="dht-complete":
        response = dht_complete(recv_message)
    elif command=="query-dht":
        response = query_dht(recv_message)
    elif command=="leave-dht":
        response = leave_dht(recv_message)
    elif command=="dht-rebuilt":
        response = dht_rebuilt(recv_message)
    elif command=="teardown-dht":
        response = teardown_dht(recv_message)
    elif command=="teardown-complete":
        response = teardown_complete(recv_message)
    elif command=="deregister":
        response = deregister(recv_message)
    elif command=="join-dht":
        response = join_dht(recv_message)    
    else:
        response = build_response_to_peer("ERROR", "Unable to resolve issue")
        
    return response



#UDP socket
recv_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

recv_socket.bind((chosen_IP, chosen_port))

while True:
    
    request = recv_socket.recvfrom(message_length)
    
    response = handle_request(request[0])
    #print(response)
    recv_socket.sendto(response, request[1])

