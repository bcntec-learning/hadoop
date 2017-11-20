echo "installing VM with proxy"
@echo off
vagrant plugin install vagrant-proxyconf
vagrant up --provision