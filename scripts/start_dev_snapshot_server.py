import os
import subprocess

if __name__ == "__main__":
    cwd = os.getcwd()

    snapshot_server_path = os.path.join(cwd, ".snapshot_server_dev")

    os.makedirs(os.path.join(snapshot_server_path, "data"), exist_ok=True)
    os.makedirs(os.path.join(snapshot_server_path, "logs"), exist_ok=True)

    subprocess.run(
        [
            "docker", "run",
            "--name", "snapshot_server_dev",
            "-p", "9000:9000",
            "-p", "9001:9001",
            "-v", f"{snapshot_server_path}/data:/data",
            "-v", f"{snapshot_server_path}/logs:/logs",
            "rustfs/rustfs:1.0.4",
        ],
        check=True,
    )