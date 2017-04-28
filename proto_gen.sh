#!/usr/bin/env bash
protoc -I=domain --go_out=domain domain/message.proto