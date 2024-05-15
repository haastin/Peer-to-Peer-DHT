import peer
import time

u = peer.Peer(peer_name="jessica", mport=13003, pport= 13004)
#IP of general5.asu.edu
#u.ipv4_address = '10.120.70.117'
u.register()
u.receive_from_peers()
time.sleep()