#!/bin/bash
set -e

echo "Starting SSH..."
sudo service ssh start

echo "Switching to mlrunner..."
exec su - mlrunner