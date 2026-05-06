const WebSocket = require('ws');
const http = require('http');
const url = require('url');

const PORT = process.env.PORT || 8080;
const server = http.createServer();
const wss = new WebSocket.Server({ noServer: true });

const rooms = new Map();

server.on('upgrade', (req, socket, head) => {
  const query = url.parse(req.url, true).query;
  const roomId = query.id;

  if (!roomId) {
    socket.write('HTTP/1.1 400 Bad Request\r\n\r\n');
    socket.destroy();
    return;
  }

  wss.handleUpgrade(req, socket, head, (ws) => {
    ws.roomId = roomId;
    wss.emit('connection', ws, req);
  });
});

wss.on('connection', (ws) => {
  const roomId = ws.roomId;
  console.log(`[server] Client connected to room: ${roomId}`);

  if (!rooms.has(roomId)) {
    rooms.set(roomId, new Set());
  }
  rooms.get(roomId).add(ws);

  ws.on('message', (data) => {
    console.log(`[server] Room ${roomId} << ${data}`);
    const room = rooms.get(roomId);
    if (room) {
      room.forEach((client) => {
        if (client !== ws && client.readyState === WebSocket.OPEN) {
          client.send(data);
        }
      });
    }
  });

  ws.on('close', () => {
    console.log(`[server] Client disconnected from room: ${roomId}`);
    const room = rooms.get(roomId);
    if (room) {
      room.delete(ws);
      if (room.size === 0) {
        rooms.delete(roomId);
      }
    }
  });

  ws.on('error', (err) => {
    console.error(`[server] Error in room ${roomId}:`, err);
  });
});

server.listen(PORT, () => {
  console.log(`[server] WebSocket server running on port ${PORT}`);
});
