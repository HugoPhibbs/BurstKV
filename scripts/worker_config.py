from pathlib import Path
import subprocess
import argparse

def build_worker_image(tag):
    build_cmd = [
        "docker", "build",
        "-f", "docker/Dockerfile.worker", 
        "-t", tag,
        "."
    ]

    project_root = Path(__file__).resolve().parent.parent
    subprocess.run(build_cmd, cwd=project_root, check=True)

def start_worker(tag, name, proxy_port, grpc_port, worker_args):
    start_cmd = [
        "docker", "run", 
        "-v", "/var/run/docker.sock:/var/run/docker.sock",
        "-p", f"{proxy_port}:{proxy_port}",
        "-p", f"{grpc_port}:{grpc_port}",
        "--network", "host", 
        "--rm",
        "--name", name,
        tag,
        "--proxy-port", str(proxy_port),
        "--grpc-port", str(grpc_port),
        *worker_args,
    ]

    subprocess.run(start_cmd, shell=True)

if __name__ == "__main__":
    parser = argparse.ArgumentParser("Build/start the worker image")
    parser.add_argument("--build", action="store_true", help="Build the image")
    parser.add_argument("--start", action="store_true", help="Start the worker container")
    parser.add_argument("--tag", "-t", default="latest", help="Worker image version")
    parser.add_argument("--name", "-n", default="burstkv-worker", help="Name of the worker container")
    parser.add_argument("--proxy-port", "-pp", type=int, default=6380)
    parser.add_argument("--grpc-port", "-gp", type=int, default=6381)
    
    args, worker_args = parser.parse_known_args()

    tag = f"burstkv-worker:{args.tag}"

    if args.build:
        build_worker_image(tag)

    elif args.start:
        start_worker(tag, args.name, args.proxy_port, args.grpc_port, worker_args)