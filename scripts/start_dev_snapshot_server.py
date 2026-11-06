import argparse
import os
import subprocess

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Start the development snapshot server.")
    parser.add_argument("--detached", "-d", action="store_true", help="Run the snapshot server in detached mode")
    parser.add_argument("--api-port", type=int, default=9000, help="Host port for the snapshot server (default: 9000)")
    parser.add_argument("--console-port", type=int, default=9001, help="Host port for the snapshot server console (default: 9001)")
    parser.add_argument("--access-key", type=str, default="dev_access_key", help="Access key for the snapshot server (default: dev_access_key)")
    parser.add_argument("--secret-key", type=str, default="dev_secret_key", help="Secret key for the snapshot server (default: dev_secret_key)")
    args = parser.parse_args()

    cwd = os.getcwd()

    snapshot_server_path = os.path.join(cwd, ".snapshot_server_dev")

    os.makedirs(os.path.join(snapshot_server_path, "data"), exist_ok=True)
    os.makedirs(os.path.join(snapshot_server_path, "logs"), exist_ok=True)

    subprocess.run(
        [
            "docker", "run",
            *(["-d"] if args.detached else []),
            "--rm",
            "--name", "snapshot_server_dev",
            "-p", f"{args.api_port}:9000",
            "-p", f"{args.console_port}:9001",
            "-v", f"{snapshot_server_path}/data:/data",
            "-v", f"{snapshot_server_path}/logs:/logs",
            "-e", f"RUSTFS_ACCESS_KEY={args.access_key}",
            "-e", f"RUSTFS_SECRET_KEY={args.secret_key}",
            "rustfs/rustfs:1.0.4",
        ],
        check=True,
    )