FROM ubuntu:14.04

RUN echo "deb http://ppa.launchpad.net/openjdk-r/ppa/ubuntu trusty main" > /etc/apt/sources.list.d/openjdk.list && \
  apt-key adv --keyserver keyserver.ubuntu.com --recv 86F44E2A && \
  apt-get update && \
  apt-get -y install openjdk-8-jre-headless && \
  rm -rf /var/lib/apt/lists/*

RUN echo "deb http://repos.mesosphere.io/ubuntu/ trusty main" > /etc/apt/sources.list.d/mesosphere.list && \
  apt-key adv --keyserver keyserver.ubuntu.com --recv E56151BF && \
  apt-get -y update && \
  apt-get -y install mesos=0.26.0-0.2.145.ubuntu1404 && \
  rm -rf /var/lib/apt/lists/*
