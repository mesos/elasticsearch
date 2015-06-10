FROM mesosphere/mesos-slave:0.22.1-1.0.ubuntu1404

# TODO Optimize

# Add dind

# Let's start with some basic stuff.
RUN apt-get update -qq && apt-get install -qqy \
    apt-transport-https \
    ca-certificates \
    curl \
    lxc \
    iptables

# Install Docker from Docker Inc. repositories.
RUN curl -sSL https://get.docker.com/ubuntu/ | sh

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
