import subprocess
import argparse

def scale_analyzers(num_analyzers, weight):
    if num_analyzers <= 0:
        print("Error: Invalid number of analyzers")
        return

    current_replicas = int(subprocess.check_output(["docker-compose", "ps", "-q", "analyzer1"]).decode().strip())
    target_replicas = num_analyzers - current_replicas  # Calculate difference

    if target_replicas > 0:
        subprocess.run(["docker-compose", "up", "-d", "--no-recreate", "--scale", f"analyzer1={num_analyzers}"])
        subprocess.run(["docker-compose", "up", "-d", "--no-recreate", "--scale", f"{analyzer_id}={1}"], env={"analyzer_WEIGHT": str(weight)}) 
    elif target_replicas < 0:
        subprocess.run(["docker-compose", "down", "--rmi", "local", "--scale", f"analyzer1={num_analyzers}"])


# python scale_analyzers.py --count 1 --weight 30
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Scale analyzers in Docker Compose')
    parser.add_argument('--count', type=int, required=True, help='The desired scale for number of analyzers')
    parser.add_argument('--weight', type=int, required=True, help='The desired weight for the analyzer')
    args = parser.parse_args()

    scale_analyzers(args.count, args.weight)


    parser = argparse.ArgumentParser(description='Scale analyzers in Docker Compose')
    subparsers = parser.add_subparsers(dest='operation', required=True)  

    # Scale Up Parser
    scale_up_parser = subparsers.add_parser('up')
    scale_up_parser.add_argument('--num_analyzers', type=int, required=True, help='Target number of analyzers')
    scale_up_parser.add_argument('--weight', type=int, required=True, help='The desired analyzer_WEIGHT')

    # Scale Down Parser
    scale_down_parser = subparsers.add_parser('down') 
    scale_down_parser.add_argument('--weight', type=int, required=True, help='The desired analyzer_WEIGHT') 

    args = parser.parse_args()

    scale_analyzers(args.operation, args.num_analyzers, args.weight)  # Pass required values or use defaults 