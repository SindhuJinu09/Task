#!/bin/bash

# Exit on error and print commands
set -ex

# Define variables
SERVICE_NAME="task-silo-service"
JAR_FILE="${SERVICE_NAME}/target/${SERVICE_NAME}-0.0.1-SNAPSHOT.jar"
REMOTE_DIR="/home/ec2-user/${SERVICE_NAME}"

# Get credentials from environment variables (set by GitHub Actions)
EC2_HOST="${TASK_SILO_SERVICE_EC2_HOST}"
PEM_CONTENT="${TASK_SILO_SERVICE_EC2_PEM}"

# Create temporary PEM file
PEM_FILE="/tmp/ec2-key.pem"
echo "$PEM_CONTENT" > "$PEM_FILE"
chmod 600 "$PEM_FILE"

# Echo present working directory
echo pwd

# Ensure the EC2 instance has the directory structure
ssh -o StrictHostKeyChecking=no -i "$PEM_FILE" "ec2-user@$EC2_HOST" "mkdir -p $REMOTE_DIR"

# Copy the JAR file to EC2
scp -o StrictHostKeyChecking=no -i "$PEM_FILE" "$JAR_FILE" "ec2-user@$EC2_HOST:$REMOTE_DIR/app.jar"

# Create service file
cat > /tmp/task-silo-service.service << EOF
[Unit]
Description=Task Service
After=network.target

[Service]
User=ec2-user
WorkingDirectory=$REMOTE_DIR
ExecStart=/usr/bin/java -jar $REMOTE_DIR/app.jar
SuccessExitStatus=143
TimeoutStopSec=10
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

# Copy service file to EC2
scp -o StrictHostKeyChecking=no -i "$PEM_FILE" /tmp/task-silo-service.service "ec2-user@$EC2_HOST:/tmp/"

# Set up and start the service
ssh -o StrictHostKeyChecking=no -i "$PEM_FILE" "ec2-user@$EC2_HOST" << 'EOF'
  sudo mv /tmp/task-silo-service.service /etc/systemd/system/
  sudo systemctl daemon-reload
  sudo systemctl enable task-silo-service.service
  sudo systemctl restart task-silo-service.service
  sudo systemctl status task-silo-service.service
EOF

# Clean up
rm -f "$PEM_FILE"

echo "Deployment of ${SERVICE_NAME} completed successfully."