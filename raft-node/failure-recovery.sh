#!/bin/bash

# Configuration
router_ip="localhost"
status_port="5000"  # Port of the router API

# Function to check if there is a leader
is_leader() {
    leader_info=$(curl -s "http://$router_ip:$status_port/leader_status")
    leader_port=$(echo "$leader_info" | jq -r '.leader_port')
    
    # Checking if the leader_port is not null or empty
    if [ -n "$leader_port" ]; then
        return 0  # There is a leader
    else
        return 1  # No leader found
    fi
}

# Function to get the current leader's port
get_leader_port() {
    leader_info=$(curl -s "http://$router_ip:$status_port/leader_status")
    leader_port=$(echo "$leader_info" | jq -r '.leader_port')
    echo "$leader_port"
}

# Starting Test
echo "Starting Failure Recovery Test..."

leader_port=$(get_leader_port)

# Step 1: Proposing a command if the leader exists
if [ -n "$leader_port" ]; then
    
    curl -X POST -H "Content-Type: application/json" -d '{"command": "initial-command"}' "http://localhost:$leader_port/log"
    echo "Initial command proposed."

    echo "Leader port is $leader_port. Killing leader..."
    
    # Step 3: Now going to kill the leader
    # Find the process ID listening on the leader port
    leader_process_id=$(lsof -t -i :$leader_port)
    
    if [ -n "$leader_process_id" ]; then
        kill -9 $leader_process_id  # Force kill for the process
        echo "Killed leader process with ID: $leader_process_id"
    else
        echo "No process found on port $leader_port. Aborting test."
        exit 1
    fi
    
    sleep 2  # Giving it a moment to notice the leader has died
    
    # Capturing the start time in nanoseconds
    start_time=$(date +%s.%N)
    # echo "Start time: $start_time"

    # Step 4: Wait for a new leader to be elected
    while true; do
        updated_leader_info=$(curl -s "http://$router_ip:$status_port/leader_status")
        updated_leader_port=$(echo "$updated_leader_info" | jq -r '.leader_port')

        if [ -n "$updated_leader_port" ] && [ "$updated_leader_port" != "$leader_port" ]; then
            break
        fi
        sleep 0.5  # Polling interval
    done

    # The end time with nanoseconds
    end_time=$(date +%s.%N)
    # echo "End time: $end_time"

    
    recovery_time=$(echo "$end_time - $start_time" | bc)
    echo "New leader elected. Recovery time: $recovery_time seconds."
else
    echo "No leader detected. Test aborted."
    exit 1
fi
