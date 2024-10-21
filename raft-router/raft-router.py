from flask import Flask, request, jsonify
import threading
import time

app = Flask(__name__)

# Raft Router global state
nodes = {}  # Dictionary of nodes registered
current_leader = None  # Node ID of the current leader
leader_last_heartbeat = 0  # Timestamp of the last heartbeat
heartbeat_timeout = 5  # Heartbeat timeout in seconds
current_term = 0  # Current term
current_leader_port=0

@app.route('/register_node', methods=['POST'])
def register_node():
    """ Register a new node to the network """
    data = request.get_json()
    node_id = data['node_id']
    
    if node_id not in nodes:
        nodes[node_id] = {'status': 'follower'}
        print(f"Node {node_id} registered.")
    
    # Return the current leader info
    return jsonify({'leader': current_leader, 'term': current_term})

@app.route('/leader_status', methods=['GET'])
def leader_status():
    """ Return the current leader status """
    return jsonify({'leader': current_leader,'leader_port': current_leader_port})

@app.route('/router-status', methods=['GET'])
def router_status():
    """ Return the router is up mor not """
    return jsonify({'message': "Hello from router!!"})

@app.route('/get-term', methods=['GET'])
def term_status():
    """ Return the current term """
    return jsonify({'term': current_term})

@app.route('/increment_term', methods=['POST'])
def increment_term():
    """ Increment the current term when a new election starts """
    global current_term
    current_term += 1
    print(f"Term incremented to {current_term}")
    return jsonify({'term': current_term})

@app.route('/leader_heartbeat', methods=['POST'])
def leader_heartbeat():
    """ Receive heartbeat from the leader """
    global leader_last_heartbeat
    global current_leader
    global current_leader_port
    data = request.get_json()
    node_id = data['node_id']
    node_port=data['node_port']

    if current_leader is None:
        current_leader = node_id
        current_leader_port=node_port
        print(f"Node {node_id} is the new leader.")
    
    if node_id == current_leader:
        leader_last_heartbeat = time.time()  # Update the last heartbeat timestamp
        print(f"Received heartbeat from leader {node_id}")
        return jsonify({'status': 'OK'})
    else:
        print(f"Received heartbeat from non-leader node {node_id}")
        return jsonify({'status': 'Error', 'message': 'Invalid leader'}), 400

@app.route('/trigger_election', methods=['POST'])
def trigger_election():
    global current_term
    current_term
    print(f"Triggering a new election for term {current_term}.")
    # Notify nodes about election using a different mechanism, such as an HTTP POST.

def monitor_heartbeats():
    """ Background thread that monitors the leader's heartbeats """
    global current_leader, leader_last_heartbeat, heartbeat_timeout
    
    while True:
        if current_leader is not None:
            if time.time() - leader_last_heartbeat > heartbeat_timeout:
                # Leader has failed (no heartbeat within timeout)
                print(f"Leader {current_leader} failed, no heartbeat for {heartbeat_timeout} seconds.")
                current_leader = None  # Reset the leader
                trigger_election()  # Notify nodes to start an election
        
        time.sleep(1)

if __name__ == "__main__":
    # Start the heartbeat monitoring thread
    threading.Thread(target=monitor_heartbeats, daemon=True).start()
    
    app.run(host='0.0.0.0', port=5000)
