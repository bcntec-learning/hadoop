#!/usr/bin/env bash
echo "installing VM with proxy"

vagrant plugin install vagrant-proxyconf
vagrant up --provision

