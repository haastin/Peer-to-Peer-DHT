import peer

t = peer.Peer(peer_name="austin", mport=13001, pport=13002)
print(t.peer_name, t.mport, t.pport)
#IP of general5.asu.edu
#t.ipv4_address = '10.120.70.117'
t.register()
t.setup_dht(3, '1950')
t.receive_from_peers()