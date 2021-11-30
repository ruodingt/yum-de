#!/bin/bash

git_hash=$(git log --pretty=format:'%h' -n 1)
echo $git_hash

read -p "Enter docker image tag name [${git_hash}]: " tag
tag=${tag:-$git_hash}

IMAGE=$(echo ruodingt7/yum-de:$tag)

echo -e build image with name:  "\e[31m$IMAGE\e[0m"
read -p "Proceed?" confirm

echo -e use image: "\e[31m$IMAGE\e[0m"
docker build -t $IMAGE .


