sudo apt-key adv --keyserver hkp://p80.pool.sks-keyservers.net:80 --recv-keys 58118E89F3A912897C070ADBF76221572C52609D
sudo vim /etc/apt/sources.list.d/docker.list
  > deb https://apt.dockerproject.org/repo ubuntu-trusty main
sudo apt-get update
sudo apt-get purge lxc-docker docker.io
sudo apt-get install linux-image-extra-$(uname -r)
sudo apt-get update
sudo apt-get install docker-engine
sudo service docker start

sudo curl -L git.io/weave -o /usr/local/bin/weave
sudo chmod a+x /usr/local/bin/weave

sudo wget -O /usr/local/bin/scope https://git.io/scope
sudo chmod a+x /usr/local/bin/scope
