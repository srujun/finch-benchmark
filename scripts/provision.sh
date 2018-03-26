#!/usr/bin/env bash

sudo apt update
sudo apt install -y python3-pip
sudo -H pip3 install --upgrade pip setuptools wheel
sudo apt install -y default-jdk
sudo echo "JAVA_HOME=\"/usr/lib/jvm/java-8-openjdk-amd64\"" >> /etc/environment
