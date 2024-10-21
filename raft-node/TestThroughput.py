import time
import requests

# Configuration
leader_url = "http://localhost:5001/log"  # URL of the Raft leader's log endpoint
num_commands =100000  # Total number of commands to send
start_time = time.time()  # Recording the start time
acknowledged = 0  # Counter for acknowledged commands
notAcknowledged = 0 # Counter for not acknowledged commands

def test_throughput():
    global acknowledged
    global notAcknowledged
    print(f"Starting throughput test: Sending {num_commands} commands to {leader_url}...\n")

    for i in range(num_commands):
        command = f"command-{i}"  # Generate a command string
        try:
            # POST request to the leader
            response = requests.post(leader_url, json={"command": command})

            
            if response.status_code in (200, 201):  # Accepts both 200 and 201 as success
                acknowledged += 1
                # print(f"[SUCCESS] Command '{command}' acknowledged. Status Code: {response.status_code}")
            else:
                # print(f"[FAILURE] Command '{command}' not acknowledged. Status Code: {response.status_code}. Response: {response.text}")
                notAcknowledged += 1

        except requests.exceptions.RequestException as e:
            print(f"[ERROR] Exception occurred while sending command '{command}': {e}")

       
        # time.sleep(0.005)  

    end_time = time.time()  # Recording the end time
    total_time = end_time - start_time  # Calculating the total duration here
    throughput = acknowledged / total_time if total_time > 0 else 0  # Calculate throughput (commands per second)

    # Print summary statistics
    print("\nThroughput test completed.")
    print(f"Total Commands Sent: {num_commands}")
    print(f"Acknowledged Commands: {acknowledged}")
    print(f"Not Acknowledged : {notAcknowledged}")
    print(f"Total Time Taken: {total_time:.2f} seconds")
    print(f"Throughput: {throughput:.2f} commands per second")

if __name__ == "__main__":
    test_throughput()
