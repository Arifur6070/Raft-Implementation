import time
import requests

# Configuration
leader_url = "http://localhost:5001/log"
num_commands = 100000  # Total number of commands I want to propose
message_count = 0  # I'll use this to track how many messages get exchanged

def log_message():
    """I'll increment the message count to track the total messages exchanged."""
    global message_count
    message_count += 1

def propose_command(command):
    """
    I'm proposing a command to the Raft leader.

    Args:
        command (str): The command I'm proposing.

    Returns:
        response: The response from the server after I propose the command.
    """
    global message_count
    try:
        # I'll send the command proposal to the leader
        response = requests.post(leader_url, json={"command": command})
        log_message()  # I'll increment the message count after proposing the command
        return response
    except requests.exceptions.RequestException as e:
        print(f"[ERROR] Failed to propose command '{command}': {e}")
        return None

def test_message_overhead():
    """I want to test how many messages are exchanged during normal operation."""
    print(f"Starting message overhead test: Proposing {num_commands} commands to the leader at {leader_url}...")

    for i in range(num_commands):
        command = f"command-{i}"  # I'll generate a unique command identifier for each command
        response = propose_command(command)  # I'll propose the command to the leader
        
        # Now I'll check the response status and log any important information
        if response is not None:
            if response.status_code in [200, 201]:
                # Success - The command was acknowledged
                pass  
            else:
                # If it failed, I'll log the failure details
                print(f"[FAILURE] Command '{command}' not acknowledged. Status Code: {response.status_code}. Response: {response.json()}")
        else:
            print(f"[FAILURE] Command '{command}' proposal failed.")

        # time.sleep(0.01)  # Optional delay between requests
    print(f"\nMessage Overhead Test Completed.")
    print(f"Total messages exchanged during normal operation: {message_count}")

if __name__ == "__main__":
    test_message_overhead()
