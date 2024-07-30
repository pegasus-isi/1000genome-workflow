FROM --platform=linux/amd64 python:3.6

RUN apt-get update -y && apt-get install -y wget curl libcurl4 openssh-client \
    && pip3 install numpy matplotlib pyyaml gitpython
