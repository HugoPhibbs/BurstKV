#!/usr/bin/env bash
sudo chown -R vscode:vscode /home/node/.claude

sudo apt-get update
sudo apt-get install -y redis-server

go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.12
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.6.2
