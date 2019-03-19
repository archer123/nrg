var app = require('express')();
var http = require('http').Server(app);
var io = require('socket.io')(http);

var resContainer = {};

app.get('/', function(req, res){
  res.sendFile(__dirname + '/index.html');
});

app.get('/send', function(req, res){
  console.log("params=",req.params);
  console.log("query=",req.query);
  io.emit('chat message', req.query);
  res.sendStatus(200);
});

app.get('/recv', function(req, res){
  console.log("params=",req.params);
  console.log("query=",req.query);
  var id = req.query.id;
  resContainer[id] = res;
});

io.on('connection', function(socket){
  
  console.log('a user connected');
  
  socket.on('chat message', function(msg){
    console.log(msg);
    io.emit('chat message', msg);
  });

  socket.on('action message', function(msg){
    if (msg.id in resContainer){
      console.log("send action", msg);
      var pendingRes = resContainer[msg.id];
      delete resContainer[msg.id];
      pendingRes.send(msg.cmd);
    } else {
      console.log("no pending request", msg);
    }
  });

  socket.on('disconnect', function(){
    console.log('user disconnected');
  });

});

http.listen(3000, function(){
  console.log('listening on *:3000');
});

