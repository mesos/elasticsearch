#!/bin/bash -e

# Docker setup
echo "Installing docker"
yum install -y docker

echo "Disabling SELinux as it causes some issues with BTRFS and we don't need it in development anyway"
if grep -Pq "^SELINUX=(enforcing|permissive)" /etc/selinux/config ; then
        sed -ri 's:^SELINUX=(enforcing|permissive):SELINUX=disabled:' /etc/selinux/config
fi
if grep -Pq "^SELINUX=(enforcing|permissive)" /etc/sysconfig/selinux ; then
	sed -ri 's:^SELINUX=(enforcing|permissive):SELINUX=disabled:' /etc/sysconfig/selinux
fi
#setenforce 0
echo "SELINUX is now disabled"

echo "Updating docker config to ignore SELinux and to accept http"
cp -f /vagrant/vagrant_files/etc/sysconfig/docker.new /etc/sysconfig/docker

echo "Adding env vars in profile.d"
cp -f /vagrant/vagrant_files/etc/profile.d/env_vars.sh /etc/profile.d/

echo "Restarting docker"
systemctl restart docker

echo "Installing docker-compose"
curl -L https://github.com/docker/compose/releases/download/1.2.0/docker-compose-`uname -s`-`uname -m` > /usr/bin/docker-compose
chmod +x /usr/bin/docker-compose

echo "Installing jdk"
yum install -y java-1.8.0-openjdk-devel.x86_64

echo "Done!"
