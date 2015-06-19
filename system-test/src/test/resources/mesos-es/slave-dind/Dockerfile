FROM ubuntu:14.10
MAINTAINER RedJack, LLC

ENV DEBIAN_FRONTEND noninteractive
ENV VERSION 0.22.0
ENV PKG_RELEASE 1.0

RUN apt-get update

# Make sure to install OpenJDK 6 explicitly.  The libmesos library includes an
# RPATH entry, which is needed to find libjvm.so at runtime.  This RPATH is
# hard-coded to the OpenJDK version that was present when the package was
# compiled.  So even though the Debian package claims that it works with either
# OpenJDK 6 or OpenJDK 7, the fact that Mesosphere compiled with OpenJDK 6 means
# that we have to have that specific version present at runtime.

WORKDIR /tmp
RUN \
  apt-get install -y curl openjdk-6-jre-headless docker.io python unzip && \
  curl -s -O https://downloads.mesosphere.io/master/ubuntu/14.04/mesos_${VERSION}-${PKG_RELEASE}.ubuntu1404_amd64.deb && \
  dpkg --unpack mesos_${VERSION}-${PKG_RELEASE}.ubuntu1404_amd64.deb && \
  apt-get install -f -y && \
  rm mesos_${VERSION}-${PKG_RELEASE}.ubuntu1404_amd64.deb && \
  apt-get clean

# TODO Optimize

# Add dind

# Let's start with some basic stuff.
RUN apt-get update -qq && apt-get install -qqy \
    apt-transport-https \
    ca-certificates \
    curl \
    openjdk-8-jdk \
    lxc \
    iptables

RUN echo deb http://get.docker.com/ubuntu docker main > /etc/apt/sources.list.d/docker.list
RUN apt-key adv --keyserver hkp://p80.pool.sks-keyservers.net:80 --recv-keys 36A1D7869245C8950F966E92D8576A8BA88D21E9
RUN apt-get update && apt-get -y install lxc-docker-1.6.0

# Install the magic wrapper.
ADD ./wrapdocker /usr/local/bin/wrapdocker
RUN chmod +x /usr/local/bin/wrapdocker

VOLUME /var/lib/docker

# Add Supervisor

# Update the base image
RUN sed -i.bak 's/main$/main universe/' /etc/apt/sources.list
RUN apt-get update && apt-get upgrade -qq

# Install Supervisord
RUN apt-get install -qq supervisor

# Make the necessary folders for Supervisord
RUN mkdir -p /var/log/supervisor /etc/supervisor/conf.d

# Add the base configuration file for Supervisord
ADD supervisor.conf /etc/supervisor.conf

CMD supervisord -c /etc/supervisor.conf
