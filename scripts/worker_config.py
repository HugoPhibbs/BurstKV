from pathlib import Path
import subprocess
import argparse

IMAGE_TAG = "burstkv-worker:latest"

def build_worker_image():
    build_cmd = [
        "docker", "build",
        "-f", "docker/Dockerfile.worker", 
        "-t", IMAGE_TAG,
        "."
    ]

    project_root = Path(__file__).resolve().parent.parent
    subprocess.run(build_cmd, cwd=project_root, check=True)

def start_worker(proxy_port, grpc_port, worker_args):
    start_cmd = [
        "docker", "run", 
        "-v", "/var/run/docker.sock:/var/run/docker.sock",
        "-p", f"{proxy_port}:{proxy_port}",
        "-p", f"{grpc_port}:{grpc_port}",
        "--network", "host", 
        IMAGE_TAG,
        "--proxy-port", str(proxy_port),
        "--grpc-port", str(grpc_port),
        *worker_args,
    ]

    subprocess.run(start_cmd, shell=True)

if __name__ == "__main__":
    parser = argparse.ArgumentParser("Build/start the worker image")
    parser.add_argument("--build-image", action="store_true", help="Build the image")
    parser.add_argument("--start-container", action="store_true", help="Start the worker container")
    parser.add_argument("--host-proxy-port", "-hpp", type=int, default=6380)
    parser.add_argument("--host-grpc-port", "-hgp", type=int, default=6381)
    
    args, worker_args = parser.parse_known_args()

    if args.build_image:
        build_worker_image()

    elif args.start_container:
        start_worker(args.proxy_port, args.grpc_port, worker_args)