#!/bin/bash

PROJECT_NAME=decode

# install requirements for lxml / cryptography
sudo apt-get install -y libxml2-dev libxslt-dev libffi-dev

source /usr/local/bin/virtualenvwrapper.sh
WORKON_HOME=~vagrant/.virtualenvs

# make sure we are in the right directory when we vagrant ssh into the vm
echo "cd /vagrant" >> /home/vagrant/.bashrc

workon $PROJECT_NAME
pip install -r /vagrant/requirements.txt
pip install honcho

