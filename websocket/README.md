Requirement
* Nodejs
* Debian 9 (optional)

# How To:
Install NodeJs & NPM modules (on client & server nodes)
```sh
curl -sL https://deb.nodesource.com/setup_10.x | sudo bash -
apt-get install nodejs -y

# server node
npm install express socket.io

# client node
npm install socket.io-client
```

Start server listen to port 9000
```sh
nodejs server.js 9000
```

From client connect to server
```sh
nodejs client.js http://127.0.0.1:9000/
```
