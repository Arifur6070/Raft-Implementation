import random
import time
import threading
import requests
import mysql.connector
from flask import Flask, request, jsonify

class RaftNode:
    def __init__(self, node_id, peers):
        self.node_id = node_id
        self.peers = peers  # List of other nodes' addresses
        self.state = "follower"
        self.current_term = 0
        self.voted_for = None
        self.log = []  # Local log
        self.commit_index = 0
        self.last_applied = 0
        self.timeout = random.uniform(1.5, 3.0)  # Timeout for leader election
        self.setup_database()
        
        # Flask setup
        self.app = Flask(__name__)

        # Register routes here
        self.app.add_url_rule('/append_entries', 'append_entries', self.append_entries, methods=['POST'])
        self.app.add_url_rule('/request_vote', 'request_vote', self.request_vote, methods=['POST'])
        self.app.add_url_rule('/submit_command', 'submit_command', self.submit_command, methods=['POST'])
        self.app.add_url_rule('/leader', 'get_leader', self.get_leader, methods=['GET'])
        self.app.add_url_rule('/get_logs', 'get_logs', self.get_logs, methods=['GET'])  
        self.app.add_url_rule('/followers', 'get_followers', self.get_followers, methods=['GET'])  # New endpoint for followers

        # Start the Flask server in a separate thread
        threading.Thread(target=self.app.run, kwargs={'port': 5000 + node_id}, daemon=True).start()

    def setup_database(self):
        """ Connect to the MySQL database """
        self.db = mysql.connector.connect(
            host="localhost",
            user="root",
            password="password",
            database="raft_db"
        )
        self.cursor = self.db.cursor()
        self.cursor.execute("CREATE TABLE IF NOT EXISTS logs (id INT AUTO_INCREMENT PRIMARY KEY, term INT NOT NULL, command VARCHAR(255) NOT NULL, timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
         # Create leader_elections table
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS leader_elections (
                id INT AUTO_INCREMENT PRIMARY KEY,
                leader_node_id INT NOT NULL,
                election_start_time VARCHAR(14) NOT NULL,
                election_duration FLOAT NOT NULL
            )
        """)

        self.db.commit()

    def send_heartbeats(self):
        start_time = time.time()  # Start timing heartbeats
        for peer in self.peers:
            if peer != f'localhost:500{self.node_id}':
                requests.post(f'http://{peer}/append_entries', json={'term': self.current_term, 'entries': []})
        
        heartbeat_time = time.time() - start_time  # Measure heartbeat latency
        print(f'Node {self.node_id} sent heartbeats in {heartbeat_time} seconds')

    def run(self):
        while True:
            if self.state == "leader":
                self.send_heartbeats()
                time.sleep(4)
            elif self.state == "follower":
                time.sleep(self.timeout)
                self.start_election()
            elif self.state == "candidate":
                self.start_election()
                time.sleep(self.timeout)

    def start_election(self):
        print(f'Node {self.node_id} is starting an election in term {self.current_term + 1}')
        start_time = time.time()  # Start timing the election
        self.state = "candidate"
        self.current_term += 1
        self.voted_for = self.node_id
        votes = 1  # Vote for self

        for peer in self.peers:
            if peer != f'localhost:500{self.node_id}':  # Avoid sending vote requests to self
                try:
                    print(f'Node {self.node_id} is requesting vote from {peer}')
                    response = requests.post(
                        f'http://{peer}/request_vote',
                        json={'term': self.current_term, 'candidate_id': self.node_id},
                        timeout=2  # Set a timeout to prevent hanging
                    )
                    if response.status_code == 200:
                        data = response.json()
                        if data['vote_granted']:
                            votes += 1
                except requests.ConnectionError:
                    print(f'Node {self.node_id} could not connect to {peer}')
                except requests.Timeout:
                    print(f'Node {self.node_id} request to {peer} timed out')

        if votes > len(self.peers) // 2:
            self.state = "leader"
            election_time = time.time() - start_time  # Measure election latency
            print(f'Node {self.node_id} became leader in term {self.current_term} after {election_time:.5f} seconds')
            
            # Format the election start time
            election_start_timestamp = time.strftime('%Y%m%d%H%M%S', time.localtime(start_time))
            
            # Insert election details into the database
            self.cursor.execute("""
                INSERT INTO leader_elections (leader_node_id, election_start_time, election_duration)
                VALUES (%s, %s, %s)
            """, (self.node_id, election_start_timestamp, election_time))
            self.db.commit()

    def get_leader(self):
        """ Endpoint to check the leader """
        return jsonify({'leader_id': self.node_id if self.state == 'leader' else None})

    def request_vote(self):
        start_time = time.time()  # Start timing
        data = request.get_json()
        term = data['term']
        candidate_id = data['candidate_id']

        if term > self.current_term:
            self.current_term = term
            self.voted_for = None
            self.state = "follower"

        vote_granted = False
        if (self.voted_for is None or self.voted_for == candidate_id) and term == self.current_term:
            self.voted_for = candidate_id
            vote_granted = True

        end_time = time.time()
        latency = end_time - start_time  # Measure request_vote latency
        print(f"Request vote processed in {latency} seconds")

        return jsonify({'term': self.current_term, 'vote_granted': vote_granted})

    def append_entries(self):
        start_time = time.time()  # Start timing
        data = request.get_json()
        term = data['term']
        entries = data.get('entries', [])

        if term < self.current_term:
            return jsonify({'term': self.current_term, 'success': False})

        self.current_term = term
        self.state = "follower"

        # Append entries to the log and database
        for entry in entries:
            self.log.append(entry)
            self.cursor.execute("INSERT INTO logs (term, command) VALUES (%s, %s)", (entry['term'], entry['command']))
            self.db.commit()

        end_time = time.time()
        latency = end_time - start_time  # Here we measeared append_entries latency
        print(f"Append entries processed in {latency} seconds")

        return jsonify({'term': self.current_term, 'success': True})
    
    def get_logs(self):
        """ Fetch logs from the database """
        self.cursor.execute("SELECT * FROM logs")
        logs = self.cursor.fetchall()  # Fetching all logs from the database
        return jsonify(logs)

    def get_followers(self):
        """ Endpoint to get the list of followers """
        if self.state == 'leader':
            # Return the peers as followers if this node is the leader
            return jsonify({'followers': [peer for peer in self.peers if peer != f'localhost:500{self.node_id}']})
        else:
            # If this node is not the leader, return an empty list or the known followers
            return jsonify({'followers': [f'localhost:500{peer_id}' for peer_id in range(len(self.peers)) if peer_id != self.node_id]})


    def submit_command(self):
        data = request.get_json()
        command = data['command']
        
        if self.state != "leader":
            return jsonify({'error': 'Only the leader can submit commands.'}), 403
        
        entry = {'term': self.current_term, 'command': command}
        self.log.append(entry)
        
        start_time = time.time()  # Started timing the log replication process

        # Here We Replicating log to followers
        for peer in self.peers:
            if peer != f'localhost:500{self.node_id}':  # Avoid sending append entries to self
                requests.post(f'http://{peer}/append_entries', json={'term': self.current_term, 'entries': [entry]})
        
        replication_time = time.time() - start_time  # Measuring log replication latency
        print(f'Command replicated across cluster in {replication_time} seconds')

        return jsonify({'term': self.current_term, 'success': True})

if __name__ == "__main__":
    import sys

    peers = ["localhost:5000", "localhost:5001", "localhost:5002", "localhost:5003", "localhost:5004", "localhost:5005"]
    nodes = [RaftNode(i, peers) for i in range(6)]  
    for node in nodes:
        node.run()
