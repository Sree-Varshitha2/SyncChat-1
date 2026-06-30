// ╔══════════════════════════════════════════════╗
// ║  ⚡ SyncChat Server  —  npm install ws uuid  ║
// ║  node server.js                              ║
// ╚══════════════════════════════════════════════╝
var WebSocket = require('ws');
var http      = require('http');
var uuid      = require('uuid');

var PORT = process.env.PORT || 8080;
var users = {};   // id -> {ws,id,username,avatar,bio,status,lastSeen}
var rooms = {};   // id -> {id,name,isGroup,members:[],messages:[],pinned:[]}
var calls = {};   // callId -> {callId,callerId,calleeId,type,status,start}
var callLog = []; // past calls

['general','tech-talk','random','announcements'].forEach(function(id,i){
  var names = ['General','Tech Talk','Random','Announcements'];
  rooms[id] = {id:id,name:names[i],isGroup:true,members:[],messages:[],pinned:[],createdAt:Date.now()};
});

var server = http.createServer(function(req,res){
  res.setHeader('Access-Control-Allow-Origin','*');
  res.setHeader('Access-Control-Allow-Methods','GET,OPTIONS');
  if(req.method==='OPTIONS'){res.writeHead(204);res.end();return;}
  if(req.url==='/health'){
    res.writeHead(200,{'Content-Type':'application/json'});
    res.end(JSON.stringify({ok:true,users:Object.keys(users).length}));
    return;
  }
  res.writeHead(404);res.end();
});

var wss = new WebSocket.Server({server:server});

function sendTo(uid,data){
  var u=users[uid];
  if(u&&u.ws.readyState===WebSocket.OPEN) u.ws.send(JSON.stringify(data));
}
function broadcast(data,excludeId){
  Object.keys(users).forEach(function(id){
    if(id!==excludeId) sendTo(id,data);
  });
}
function toRoom(roomId,data,excludeId){
  var r=rooms[roomId];
  if(r) r.members.forEach(function(id){ if(id!==excludeId) sendTo(id,data); });
}
function userList(){
  return Object.values(users).map(function(u){
    return {id:u.id,username:u.username,avatar:u.avatar,bio:u.bio,status:u.status,lastSeen:u.lastSeen};
  });
}
function roomList(){
  return Object.values(rooms).map(function(r){
    return {id:r.id,name:r.name,isGroup:r.isGroup,members:r.members,
            lastMessage:r.messages[r.messages.length-1]||null,createdAt:r.createdAt,pinned:r.pinned};
  });
}

wss.on('connection',function(ws){
  ws.on('message',function(raw){
    var data;
    try{data=JSON.parse(raw);}catch(e){return;}
    var type=data.type, p=data.payload||{};

    if(type==='join'){
      var uid=uuid.v4();
      ws._uid=uid;
      users[uid]={ws:ws,id:uid,username:p.username,avatar:p.avatar||'😎',
                  bio:p.bio||'',status:'online',lastSeen:Date.now()};
      var gen=rooms['general'];
      if(gen.members.indexOf(uid)===-1) gen.members.push(uid);
      ws.send(JSON.stringify({type:'welcome',payload:{
        userId:uid,username:p.username,
        users:userList(),rooms:roomList(),
        messages:gen.messages.slice(-60),callLog:callLog.slice(-20)
      }}));
      broadcast({type:'user_joined',payload:users[uid]&&{
        id:uid,username:users[uid].username,avatar:users[uid].avatar,
        bio:users[uid].bio,status:'online',lastSeen:Date.now()
      }},uid);
      return;
    }

    var me=users[ws._uid];
    if(!me) return;

    if(type==='message'){
      var r=rooms[p.roomId]; if(!r) return;
      var msg={id:uuid.v4(),roomId:p.roomId,senderId:me.id,senderName:me.username,
               senderAvatar:me.avatar,text:p.text||'',replyTo:p.replyTo||null,
               fileData:p.fileData||null,fileName:p.fileName||null,fileType:p.fileType||null,
               forwardedFrom:p.forwardedFrom||null,timestamp:Date.now(),
               status:'sent',reactions:{},edited:false,poll:p.poll||null};
      r.messages.push(msg);
      if(r.messages.length>500) r.messages.shift();
      toRoom(p.roomId,{type:'message',payload:msg});
      setTimeout(function(){
        msg.status='delivered';
        toRoom(p.roomId,{type:'msg_status',payload:{msgId:msg.id,status:'delivered'}});
      },300);
      return;
    }

    if(type==='dm'){
      var key=[me.id,p.toId].sort().join('_');
      if(!rooms[key]) rooms[key]={id:key,name:'DM',isGroup:false,members:[me.id,p.toId],messages:[],pinned:[],createdAt:Date.now()};
      var dm=rooms[key];
      var dmsg={id:uuid.v4(),roomId:key,senderId:me.id,senderName:me.username,
                senderAvatar:me.avatar,text:p.text||'',replyTo:p.replyTo||null,
                fileData:p.fileData||null,fileName:p.fileName||null,fileType:p.fileType||null,
                forwardedFrom:p.forwardedFrom||null,timestamp:Date.now(),
                status:'sent',reactions:{},edited:false};
      dm.messages.push(dmsg);
      sendTo(me.id,{type:'dm_message',payload:dmsg});
      sendTo(p.toId,{type:'dm_message',payload:dmsg});
      sendTo(p.toId,{type:'dm_room',payload:{room:{id:key,isGroup:false,members:[me.id,p.toId]},message:dmsg}});
      return;
    }

    if(type==='load_dm'){
      var k2=[me.id,p.toId].sort().join('_');
      var dr=rooms[k2];
      sendTo(me.id,{type:'dm_history',payload:{roomId:k2,messages:dr?dr.messages.slice(-60):[]}});
      return;
    }

    if(type==='join_room'){
      var jr=rooms[p.roomId]; if(!jr) return;
      if(jr.members.indexOf(me.id)===-1) jr.members.push(me.id);
      sendTo(me.id,{type:'room_history',payload:{roomId:jr.id,messages:jr.messages.slice(-60),pinned:jr.pinned}});
      toRoom(jr.id,{type:'room_joined',payload:{roomId:jr.id,userId:me.id,username:me.username}},me.id);
      return;
    }

    if(type==='typing'){
      toRoom(p.roomId,{type:'typing',payload:{userId:me.id,username:me.username,on:p.on,roomId:p.roomId}},me.id);
      return;
    }

    if(type==='read'){
      var rr=rooms[p.roomId]; if(!rr) return;
      p.msgIds.forEach(function(mid){
        var m=rr.messages.find(function(x){return x.id===mid;}); if(m) m.status='read';
      });
      toRoom(p.roomId,{type:'read_receipt',payload:{roomId:p.roomId,msgIds:p.msgIds,readBy:me.id}},me.id);
      return;
    }

    if(type==='react'){
      var reactRoom=rooms[p.roomId]; if(!reactRoom) return;
      var rm=reactRoom.messages.find(function(x){return x.id===p.msgId;}); if(!rm) return;
      if(!rm.reactions[p.emoji]) rm.reactions[p.emoji]=[];
      var ri=rm.reactions[p.emoji].indexOf(me.id);
      if(ri===-1) rm.reactions[p.emoji].push(me.id);
      else rm.reactions[p.emoji].splice(ri,1);
      toRoom(p.roomId,{type:'reaction',payload:{msgId:p.msgId,reactions:rm.reactions,roomId:p.roomId}});
      return;
    }

    if(type==='edit_msg'){
      var er=rooms[p.roomId]; if(!er) return;
      var em=er.messages.find(function(x){return x.id===p.msgId&&x.senderId===me.id;}); if(!em) return;
      em.text=p.text; em.edited=true;
      toRoom(p.roomId,{type:'msg_edited',payload:{msgId:em.id,text:em.text,roomId:p.roomId}});
      return;
    }

    if(type==='delete_msg'){
      var delRoom=rooms[p.roomId]; if(!delRoom) return;
      if(p.forMe){
        sendTo(me.id,{type:'msg_deleted',payload:{msgId:p.msgId,roomId:p.roomId,forMe:true}});
      } else {
        var di=delRoom.messages.findIndex(function(x){return x.id===p.msgId&&x.senderId===me.id;});
        if(di===-1) return;
        delRoom.messages.splice(di,1);
        toRoom(p.roomId,{type:'msg_deleted',payload:{msgId:p.msgId,roomId:p.roomId,forMe:false}});
      }
      return;
    }

    if(type==='pin_msg'){
      var pr=rooms[p.roomId]; if(!pr) return;
      var pm=pr.messages.find(function(x){return x.id===p.msgId;}); if(!pm) return;
      pm.pinned=!pm.pinned;
      if(pm.pinned) pr.pinned.push({id:pm.id,text:pm.text,senderName:pm.senderName});
      else pr.pinned=pr.pinned.filter(function(x){return x.id!==pm.id;});
      toRoom(p.roomId,{type:'pin_update',payload:{msgId:pm.id,pinned:pm.pinned,pins:pr.pinned,roomId:p.roomId}});
      return;
    }

    if(type==='forward_msg'){
      var fm=null;
      Object.values(rooms).forEach(function(r2){
        if(!fm) fm=r2.messages.find(function(x){return x.id===p.msgId;});
      });
      if(!fm) return;
      p.toRoomIds.forEach(function(rid){
        var tr=rooms[rid]; if(!tr) return;
        var fwd=JSON.parse(JSON.stringify(fm));
        fwd.id=uuid.v4(); fwd.roomId=rid;
        fwd.senderId=me.id; fwd.senderName=me.username; fwd.senderAvatar=me.avatar;
        fwd.forwardedFrom=fm.senderName; fwd.timestamp=Date.now(); fwd.status='sent'; fwd.reactions={};
        tr.messages.push(fwd);
        toRoom(rid,{type:'message',payload:fwd});
      });
      return;
    }

    if(type==='status'){
      me.status=p.status; me.lastSeen=Date.now();
      broadcast({type:'user_status',payload:{userId:me.id,status:me.status,lastSeen:me.lastSeen}});
      return;
    }

    if(type==='poll_vote'){
      var pvr=rooms[p.roomId]; if(!pvr) return;
      var pvm=pvr.messages.find(function(x){return x.id===p.msgId;}); if(!pvm||!pvm.poll) return;
      pvm.poll.options.forEach(function(o){
        var vi=o.votes.indexOf(me.id); if(vi>-1) o.votes.splice(vi,1);
      });
      var pvo=pvm.poll.options[p.optIdx];
      if(pvo){ pvo.votes.push(me.id); pvm.poll.total=pvm.poll.options.reduce(function(a,o){return a+o.votes.length;},0); }
      toRoom(p.roomId,{type:'poll_update',payload:{msgId:pvm.id,poll:pvm.poll,roomId:p.roomId}});
      return;
    }

    // ── CALLS ─────────────────────────────────────────────
    if(type==='call_offer'){
      var cid=uuid.v4();
      calls[cid]={callId:cid,callerId:me.id,calleeId:p.toId,type:p.callType,status:'ringing',start:Date.now()};
      sendTo(p.toId,{type:'incoming_call',payload:{callId:cid,callerId:me.id,callerName:me.username,callerAvatar:me.avatar,callType:p.callType}});
      sendTo(me.id,{type:'call_initiated',payload:{callId:cid}});
      return;
    }
    if(type==='call_answer'){
      var ca=calls[p.callId]; if(!ca) return;
      ca.status=p.accepted?'active':'rejected';
      if(p.accepted) ca.answerTime=Date.now();
      sendTo(ca.callerId,{type:p.accepted?'call_accepted':'call_rejected',payload:{callId:p.callId}});
      if(!p.accepted){ callLog.push({callId:p.callId,callerId:ca.callerId,calleeId:ca.calleeId,type:ca.type,status:'rejected',duration:0,time:Date.now()}); delete calls[p.callId]; }
      return;
    }
    if(type==='call_ice'){
      var ci=calls[p.callId]; if(!ci) return;
      var peerId=me.id===ci.callerId?ci.calleeId:ci.callerId;
      sendTo(peerId,{type:'call_ice',payload:{callId:p.callId,candidate:p.candidate}});
      return;
    }
    if(type==='call_end'){
      var ce=calls[p.callId]; if(!ce) return;
      var dur=ce.answerTime?Math.floor((Date.now()-ce.answerTime)/1000):0;
      var peer2=me.id===ce.callerId?ce.calleeId:ce.callerId;
      sendTo(peer2,{type:'call_ended',payload:{callId:p.callId,duration:dur}});
      sendTo(me.id,{type:'call_ended',payload:{callId:p.callId,duration:dur}});
      callLog.push({callId:p.callId,callerId:ce.callerId,calleeId:ce.calleeId,type:ce.type,status:'completed',duration:dur,time:Date.now()});
      if(callLog.length>50) callLog.shift();
      delete calls[p.callId];
      broadcast({type:'call_log_update',payload:{callLog:callLog.slice(-20)}});
      return;
    }

    if(type==='ping'){ sendTo(me.id,{type:'pong',payload:{ts:Date.now()}}); }
  });

  ws.on('close',function(){
    var uid=ws._uid; if(!uid) return;
    var u=users[uid]; if(!u) return;
    broadcast({type:'user_status',payload:{userId:uid,status:'offline',lastSeen:Date.now()}});
    Object.values(rooms).forEach(function(r){ var i=r.members.indexOf(uid); if(i>-1) r.members.splice(i,1); });
    delete users[uid];
  });

  ws.on('error',function(e){ console.error('WS error',e.message); });
});

server.listen(PORT,function(){ console.log('⚡ SyncChat ws://localhost:'+PORT); });
