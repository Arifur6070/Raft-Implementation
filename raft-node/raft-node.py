import os
import requests
import time
import threading
import random
import mysql.connector
from flask import Flask, request, jsonify

class RaftNode:
    def __init__(self, node_id, node_port, router_address, db_config):
        self.node_id = node_id
        self.node_port = node_port
        self.router_address = router_address
        self.state = "follower"
        self.current_term = 0
        self.voted_for = None
        self.timeout = random.uniform(1.5, 3.0)  # Randomized election timeout
        self.leader_heartbeat_interval = 2  # Leader sends heartbeat every 2 seconds
        self.is_leader = False
        self.setup_flask()

        # Initialize MySQL connection
        self.db_connection = mysql.connector.connect(**db_config)
        self.db_cursor = self.db_connection.cursor()

        # Register with the Raft Router
        self.register_with_router()

        # Start monitoring the leader and heartbeats
        threading.Thread(target=self.monitor_leader, daemon=True).start()

    def setup_flask(self):
        """ Set up Flask for handling requests. """
        self.app = Flask(__name__)
        self.app.add_url_rule('/request_vote', 'request_vote', self.request_vote, methods=['POST'])
        self.app.add_url_rule('/node-status', 'get_node_status', self.get_node_status, methods=['GET'])
        self.app.add_url_rule('/log', 'log', self.store_log, methods=['POST'])  # route for logs
        self.app.add_url_rule('/check_consistency', 'check_consistency', self.check_consistency, methods=['GET'])  # Consistency check route

    def register_with_router(self):
        """ Register this node with the Raft Router """
        response = requests.post(f'http://{self.router_address}/register_node', json={'node_id': self.node_id})
        data = response.json()
        leader = data.get('leader')
        self.current_term = data.get('term', 0)
        if leader is None:
            print(f"Node {self.node_id} sees no leader, starting an election.")
            self.start_election()

    def start_election(self):
        """ Start election process """
        print(f"Node {self.node_id} starting election for term {self.current_term + 1}")
        self.state = "candidate"

        # Increment the term by requesting the router to do so
        response = requests.post(f'http://{self.router_address}/increment_term')
        term_data = response.json()
        self.current_term = term_data['term']

        self.voted_for = self.node_id
        
        self.become_leader()

    def become_leader(self):
        """ Node becomes leader """
        print(f"Node {self.node_id} became leader for term {self.current_term}")
        self.is_leader = True
        self.state = "leader"

        # initial heartbeat as the new leader
        requests.post(f'http://{self.router_address}/leader_heartbeat', json={'node_id': self.node_id,'node_port': self.node_port})

        # Sending heartbeats periodically
        threading.Thread(target=self.send_heartbeats, daemon=True).start()

    def send_heartbeats(self):
        """ Send heartbeats to the Raft Router if this node is the leader """
        while self.is_leader:
            try:
                requests.post(f'http://{self.router_address}/leader_heartbeat', json={'node_id': self.node_id,'node_port': self.node_port})
                print(f"Leader {self.node_id} sent heartbeat.")
            except Exception as e:
                print(f"Failed to send heartbeat: {e}")
            time.sleep(self.leader_heartbeat_interval)

    def monitor_leader(self):
        """ Periodically check for heartbeats and initiate an election if needed """
        while True:
            if not self.is_leader:
                try:
                    response = requests.get(f'http://{self.router_address}/leader_status')
                    data = response.json()
                    leader = data.get('leader')
                    
                    if leader is None:
                        print(f"Node {self.node_id} detected no leader, starting election.")
                        self.start_election()
                    else:
                        print(f"Node {self.node_id} detected leader {leader}.")
                except Exception as e:
                    print(f"Failed to check leader status: {e}")
            
            time.sleep(2)

    def request_vote(self):
        """ Handle vote requests from other nodes """
        data = request.get_json()
        term = data['term']
        candidate_id = data['candidate_id']
        # Vote logic here
        return jsonify({'term': self.current_term, 'vote_granted': True})
    
    def get_node_status(self):
        """ Handle vote requests from other nodes """
        return jsonify({'message': "Hello From Node", 'node_id': self.node_id})

    def store_log(self):
        """ Store a log entry in the MySQL database without requiring the term from the user """
        data = request.get_json()
        command = data['command']

        # Using the current term of the leader
        term = self.current_term

        # Inserting log entry into MySQL
        try:
            sql = "INSERT INTO logs (term, command) VALUES (%s, %s)"
            self.db_cursor.execute(sql, (term, command))
            self.db_connection.commit()
            return jsonify({'status': 'success', 'message': 'Log stored successfully.', 'term': term}), 201
        except mysql.connector.Error as err:
            print(f"Error: {err}")
            return jsonify({'status': 'error', 'message': 'Failed to store log.'}), 500

    def check_consistency(self):
        self.db_cursor.execute("SELECT * FROM logs ORDER BY id")
        logs = self.db_cursor.fetchall()
        return jsonify({'logs': logs})

    def run(self, host='0.0.0.0', port=5000):
        self.app.run(host=host, port=port)

if __name__ == "__main__":
    node_id = int(os.getenv("NODE_ID", 1))
    # node_id = int(input("Enter Node ID: "))
    # node_port = int(input("Enter Port for this node: "))
    router_address = "172.18.0.3:5000"  # Raft Router address
    
    # Database configuration
    db_config = {
        'user': 'root',
        'password': 'password',
        'host': '172.18.0.2',
        'port': 3306,
        'database': 'raft_db'
    }
    
    node = RaftNode(node_id, 5000, router_address, db_config)
    node.run()
