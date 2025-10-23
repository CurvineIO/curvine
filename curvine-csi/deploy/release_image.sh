#!/bin/bash
set -e
set -x

# AWS ECR login
aws --profile ml-prod ecr get-login-password --region us-east-2 | docker login --username AWS --password-stdin 050752647787.dkr.ecr.us-east-2.amazonaws.com

# Push image to AWS ECR

docker tag docker.io/curvine/csi:latest 050752647787.dkr.ecr.us-east-2.amazonaws.com/curvine-csi:latest 
docker push 050752647787.dkr.ecr.us-east-2.amazonaws.com/curvine-csi:latest 