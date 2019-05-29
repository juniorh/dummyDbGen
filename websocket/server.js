// npm install express socket.io
// nodejs server.js <PORT>
// e.g: nodejs server.js 9000

var q = [];
var port = process.argv[2] || 3000
var app = require('express')();
var http = require('http').Server(app);
var io = require('socket.io')(http);
var path = require('path')
var dir = path.resolve()

app.get('/', function(req, res){
  q.req=req;
  q.res=res;
  res.sendFile(dir+'/index.html');
});

io.on('connection', function(socket){
  q.socket = socket;
  //console.log('a user connected: '+socket.id+' nClient: '+q.socket.server.engine.clientsCount.toString());
  socket.on('disconnect', function(){
    console.log('user disconnected: '+socket.id);
  });
  socket.on('ch1', function(msg){
        q.msg = msg;
    console.log('message from '+socket.id+' :' + msg);
  });
});

var main = function(){
  console.log('listening on *:'+port);
}

q.http = http.listen(port, main);

