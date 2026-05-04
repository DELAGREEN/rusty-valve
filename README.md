docker run -d --name mosquitto-test -p 1883:1883 -p 9001:9001 eclipse-mosquitto

docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' mosquitto-test

docker rm -f mqtt-broker
docker run -d --name mqtt-broker -p 1883:1883 --privileged eclipse-mosquitto

### Для qr
sudo dnf install zbar