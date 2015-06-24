#!/bin/bash -e

echo "Running system updates"
sudo yum clean all
sudo yum -y --skip-broken update

echo "Installing docker"
curl -O -sSL https://get.docker.com/rpm/1.7.0/centos-7/RPMS/x86_64/docker-engine-1.7.0-1.el7.centos.x86_64.rpm
yum -y localinstall --nogpgcheck docker-engine-1.7.0-1.el7.centos.x86_64.rpm

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

echo "Adding host entry for docker.io"
echo "127.0.0.1 docker.io" > /etc/hosts

echo "Restarting docker"
# Sometimes restart doesn't work if it hasn't started yet.
systemctl stop docker
systemctl start docker

echo "Adding vagrant to docker group"
sudo usermod -aG docker vagrant
sudo chgrp docker /var/run/docker.sock

echo "Installing docker-compose"
curl -L https://github.com/docker/compose/releases/download/1.2.0/docker-compose-`uname -s`-`uname -m` > /usr/bin/docker-compose
chmod +x /usr/bin/docker-compose

echo "Installing jdk"
yum install -y java-1.8.0-openjdk-devel.x86_64

echo "Done!"
