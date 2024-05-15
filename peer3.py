import peer

v = peer.Peer(peer_name="kayla", mport=13005, pport= 13006)
#IP of general5.asu.edu
#v.ipv4_address = '10.120.70.117'
v.register()
v.receive_from_peers()