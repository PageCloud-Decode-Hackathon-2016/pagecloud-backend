# -*- mode: ruby -*-
# vi: set ft=ruby :

SYNCED_FOLDER = "/vagrant"

Vagrant.require_version ">= 1.6.3"

Vagrant.configure("2") do |config|
  config.vm.synced_folder ".", SYNCED_FOLDER, type: "nfs"

  # database server:
  config.vm.define :"db" do |db|
    db.vm.hostname = "db"
    db.vm.box = "https://dl.dropboxusercontent.com/s/6xclwgyoedtzhjx/pagecloud-base-db-v0.4.box"
    db.vm.network :private_network, ip: "192.168.50.3"
    db.vm.provision :shell, :path => "vagrant_data/db/bootstrap.sh"
    db.vm.network "forwarded_port", guest: 15432, host: 15432
  end

  # Web server:
  config.vm.define :"web", primary: true do |web|
    web.vm.hostname = "web"
    #web.vm.box = "https://dl.dropboxusercontent.com/s/bm014kht20kkryd/pagecloud-base-web-v0.4.box"
    web.vm.box = "https://www.dropbox.com/s/cn5h27nnprq1vmu/pagecloud-base-web-v0.5.2.box?dl=1"
    web.vm.provision :shell, :path => "vagrant_data/web/bootstrap.sh"
    web.vm.network :private_network, ip: "192.168.50.4"
    web.vm.network "forwarded_port", guest: 5000, host: 5000
    web.vm.provider "virtualbox" do |v|
      v.customize [
        "modifyvm", :id,
        "--memory", "512",
        "--cpus", "1"
      ]
    end
  end

end
