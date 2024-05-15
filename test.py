import subprocess
import time

def send_data(data, peer):
    sending = (data+'\n').encode()
    print(f"sending {sending}")
    peer.stdin.write(sending)
    peer.stdin.flush()
    print("FLUSHED")
    return

manager = subprocess.Popen(["python3", "manager.py", "13000"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
time.sleep(.5)
peer1 = subprocess.Popen(["python3", "peer.py", "127.0.0.1", "13000"], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
time.sleep(.2)
peer2 = subprocess.Popen(["python3", "peer.py", "127.0.0.1", "13000"], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
time.sleep(.2)
peer3 = subprocess.Popen(["python3", "peer.py", "127.0.0.1", "13000"], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
time.sleep(.2)
setupdht_peer = subprocess.Popen(["python3", "peer.py", "127.0.0.1", "13000"], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

register = "register"

setup_dht = "setup-dht"
n = "3"
year = "1950"

query_dht = "query-dht"
event_id = "3"
time.sleep(.5)
send_data(register, peer1)
send_data("a", peer1)

send_data(register, peer2)
send_data("b", peer2)

send_data(register, setupdht_peer)
send_data("d", setupdht_peer)
send_data(setup_dht, setupdht_peer)
send_data(n, setupdht_peer)
send_data(year, setupdht_peer)

send_data(register, peer3)
send_data("c", peer3)
send_data(query_dht, peer3)
send_data(event_id, peer3)

input()

manager.kill()
peer1.kill()
peer2.kill()
peer3.kill()
setupdht_peer.kill()

print(f"manager output:\n\n {manager.stderr.read().decode('utf-8')}\n\n{manager.stdout.read().decode('utf-8')}\n\npeer1 output:\n\n {peer1.stderr.read().decode('utf-8')}\n\n {peer1.stdout.read().decode('utf-8')}\n\npeer2 output:\n\n {peer2.stderr.read().decode('utf-8')}\n\n {peer2.stdout.read().decode('utf-8')}\n\nsetupdht_peer output:\n\n {setupdht_peer.stderr.read().decode('utf-8')}\n\n {setupdht_peer.stdout.read().decode('utf-8')}\n\n")
