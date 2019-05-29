// npm install socket.io-client
// nodejs client.js http://<IPDEST>:<PORTDEST>/
// e.g: nodejs client.js http://192.168.10.20:9000/

var io = require('socket.io-client');
var target = process.argv[2];
var socket = io.connect(target, {reconnect: true});
var q = [];
q.socket = socket;
socket.on('connect', function (socket) {
    q.socketon = socket;
    console.log('Connected on! '+q.socket.io.engine.id);
    setTimeout(function sendmessage() {
        q.socket.emit('ch1', 'testing');
        setTimeout(sendmessage, 1000)
    }, 1000);
});
socket.on('event', function (socket) {
    q.socketevent = socket;
    console.log('rcv event! '+q.socket.io.engine.id);
});
socket.on('disconnect', function (d) {
    q.socketdc=d
    console.log('Disconnected! '+d);
});

