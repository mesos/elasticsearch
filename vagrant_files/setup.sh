#!/bin/bash -e

# Docker setup
echo "Installing docker"
yum install -y docker

echo "Disabling SELinux as it causes some issues with BTRFS and we don't need it in development anyway"
# Disable SELinux
if grep -Pq "^SELINUX=(enforcing|permissive)" /etc/selinux/config ; then
        sed -ri 's:^SELINUX=(enforcing|permissive):SELINUX=disabled:' /etc/selinux/config
fi
if grep -Pq "^SELINUX=(enforcing|permissive)" /etc/sysconfig/selinux ; then
	sed -ri 's:^SELINUX=(enforcing|permissive):SELINUX=disabled:' /etc/sysconfig/selinux
fi
setenforce 0
echo "SELINUX is now disabled"

echo "Updating docker config to ignore SELinux and to accept http"
cp -f /vagrant/vagrant_files/etc/sysconfig/docker.new /etc/sysconfig/docker

echo "Restarting docker"
systemctl restart docker

echo "Installing jdk"
yum install -y java-1.8.0-openjdk-devel.x86_64

echo "Setting JAVA_HOME"
export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.31-7.b13.el7_1.x86_64
echo "JAVA_HOME set to $JAVA_HOME"

echo "Done!"
