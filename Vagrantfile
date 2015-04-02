Vagrant.configure('2') do |config|

  # Every Vagrant virtual environment requires a box to build off of.
  config.vm.box = 'centos70'
  
  config.ssh.forward_agent = true

  config.vm.box_url = 'http://deltaforce.io/box/centos70-base_1414068169.box'
  config.vm.provider :virtualbox do |vb|
    vb.customize ['modifyvm', :id, '--memory', 4096]
    vb.customize ['modifyvm', :id, '--ioapic', 'on']
  end

  config.vm.provision :shell do |external_shell|
    external_shell.path = 'setup.sh'
  end
end
