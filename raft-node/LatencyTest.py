import requests
import time

def test_latency(node_port, commands):
    latency_results = []
    for command in commands:
        try:
            start_time = time.time()  # Starting point of the timer
            response = requests.post(f'http://localhost:{node_port}/log', json={'command': command})
            latency = time.time() - start_time  # Calculating the latency

            
            if response.status_code == 201:
                print(f"Successfully proposed command '{command}'. Latency: {latency:.4f} seconds.")
                latency_results.append(latency)  # Collecting latency data
            else:
                print(f"Failed to propose command '{command}'. Status code: {response.status_code}. Response: {response.text}")

        except Exception as e:
            print(f"Error proposing command '{command}': {e}")

    # Calculating the average latency
    if latency_results:
        average_latency = sum(latency_results) / len(latency_results)
        print(f"Average Latency: {average_latency:.4f} seconds.")
    else:
        print("No successful proposals made, cannot calculate average latency.")

if __name__ == "__main__":
    node_port = int(input("Enter Node Port: "))  
    commands = ['Crossaint', 'Brot', 'Kuchen']  # Example commands for proposing
    test_latency(node_port, commands)
