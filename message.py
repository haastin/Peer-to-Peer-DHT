import struct
import json

message_delimiter = b'\r\n'
params_delimiter = '\n'

class PeertoManagerMessage():

    #used to assign fixed byte size to each variable in our object (this is the format for the struct.pack call)
    format_string = '20s 15s 15s L L Q 4s 15s'
    message_length = struct.calcsize(format_string)

    def __init__(self, command, peer_name="", peer_ipv4_address="", peer_mport=0, peer_pport=0, n=0, year="", new_leader=""):

        self.command = command
        #all commands use peer_name
        self.peer_name = peer_name[:15]

        #register params
        self.peer_ipv4_address = peer_ipv4_address
        self.peer_mport = peer_mport
        self.peer_pport = peer_pport

        #setup-dht params
        self.n=n
        self.year=year

        #dht-rebuilt params
        self.new_leader=new_leader[:15]
    
    def pack_message(self):
        return struct.pack(PeertoManagerMessage.format_string, self.command.encode('utf-8'), self.peer_name.encode('utf-8'), self.peer_ipv4_address.encode('utf-8'), self.peer_mport, self.peer_pport, self.n, self.year.encode('utf-8'), self.new_leader.encode('utf-8'))

    def unpack_message(message_in_byteform):
        command, peer_name, peer_ipv4_address, peer_mport, peer_pport, n, year, new_leader = struct.unpack(PeertoManagerMessage.format_string, message_in_byteform)
       
       #after unpacking, there can still be null bytes used for padding, so we get rid of those
        command = command.rstrip(b'\x00').decode('utf-8')
        peer_name = peer_name.rstrip(b'\x00').decode('utf-8')
        peer_ipv4_address = peer_ipv4_address.rstrip(b'\x00').decode('utf-8')
        year = year.rstrip(b'\x00').decode('utf-8')
        new_leader = new_leader.rstrip(b'\x00').decode('utf-8')

        return PeertoManagerMessage(command, peer_name, peer_ipv4_address, peer_mport, peer_pport, n,year,new_leader)
    

# me = PeertoManagerMessage("austin", peer_pport=4)
# print(me.command, me.peer_pport)
# me = me.pack_message()
# print(me)
# print(len(me))
# me = PeertoManagerMessage.unpack_message(me)
# print(me.command, me.peer_pport)

class PeertoPeerMessage():

    format_string = '20s L L L L B'
    message_length = struct.calcsize(format_string)

    def __init__(self, command, pos_of_record=0, identifier=0, ring_size=0, event_id=0,additional_parameters=False):
        self.command = command
        self.pos_of_record = pos_of_record
        self.identifier = identifier
        self.ring_size = ring_size
        self.event_id= event_id
        self.additional_parameters = additional_parameters
        
    def pack_message(self):
        return struct.pack(PeertoPeerMessage.format_string, self.command.encode('utf-8'), self.pos_of_record, self.identifier, self.ring_size, self.event_id, self.additional_parameters)
    
    def unpack_message(message_in_byteform):
        command, pos_of_record, identifier, ring_size, event_id, additional_parameters = struct.unpack(PeertoPeerMessage.format_string, message_in_byteform)

        command = command.rstrip(b'\x00').decode('utf-8')

        return PeertoPeerMessage(command=command, pos_of_record=pos_of_record, identifier=identifier, ring_size=ring_size, event_id=event_id,additional_parameters=additional_parameters)


# me = PeertoPeerMessage("austin", 4)
# print(me.command, me.pos_of_record)
# me = me.pack_message()
# print(me)
# print(len(me))
# me = PeertoPeerMessage.unpack_message(me)
# print(me.command, me.pos_of_record)
    
class Response():
    def __init__(self, parameters):
        self.params = list(parameters)

    def build_response(self):
        
        #convert everything to json and join the parameters with the pre-defined delimiter
        response = (json.dumps(self.params)).encode('utf-8') + message_delimiter
        return response