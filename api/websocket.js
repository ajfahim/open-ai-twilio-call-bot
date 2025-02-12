import { WebSocketServer } from 'ws';
import dotenv from 'dotenv';
import WebSocket from 'ws';

dotenv.config();

export default function handler(req, res) {
  if (req.method === 'GET') {
    if (!res.socket.server.wss) {
      // Initialize the WebSocket server
      res.socket.server.wss = new WebSocketServer({ noServer: true });
      
      res.socket.server.wss.on('connection', function connection(ws) {
        console.log('Client connected');
        let streamSid = null;
        let latestMediaTimestamp = 0;
        let lastAssistantItem = null;
        let markQueue = [];
        let responseStartTimestampTwilio = null;

        // Initialize OpenAI WebSocket
        const openAiWs = new WebSocket(
          "wss://api.openai.com/v1/realtime?model=gpt-4o-realtime-preview-2024-10-01",
          {
            headers: {
              Authorization: `Bearer ${process.env.OPENAI_API_KEY}`,
              "OpenAI-Beta": "realtime=v1",
            },
          }
        );

        // Handle OpenAI WebSocket events
        openAiWs.on('open', () => {
          console.log('Connected to OpenAI');
          const sessionUpdate = {
            type: "session.update",
            session: {
              turn_detection: { type: "server_vad" },
              input_audio_format: "g711_ulaw",
              output_audio_format: "g711_ulaw",
              voice: "alloy",
              instructions: "You are a helpful and bubbly AI assistant who loves to chat about anything the user is interested about and is prepared to offer them facts.",
              modalities: ["text", "audio"],
              temperature: 0.8,
            },
          };
          openAiWs.send(JSON.stringify(sessionUpdate));
        });

        openAiWs.on('message', (data) => {
          try {
            const response = JSON.parse(data);
            if (response.type === "response.audio.delta" && response.delta) {
              ws.send(JSON.stringify({
                event: "media",
                streamSid: streamSid,
                media: { payload: response.delta },
              }));
            }
          } catch (error) {
            console.error('Error processing OpenAI message:', error);
          }
        });

        // Handle client messages
        ws.on('message', function incoming(message) {
          try {
            const data = JSON.parse(message);
            if (data.event === "start") {
              streamSid = data.start.streamSid;
              console.log("Media WS: Received start event");
            }

            if (data.event === "media") {
              latestMediaTimestamp = data.media.timestamp;
              if (openAiWs.readyState === WebSocket.OPEN) {
                const audioAppend = {
                  type: "input_audio_buffer.append",
                  audio: data.media.payload,
                };
                openAiWs.send(JSON.stringify(audioAppend));
              }
            }
          } catch (error) {
            console.error('Error processing client message:', error);
          }
        });

        ws.on('close', function close() {
          console.log('Client disconnected');
          if (openAiWs.readyState === WebSocket.OPEN) {
            openAiWs.close();
          }
        });
      });
    }
    res.end();
  } else {
    res.status(405).json({ error: 'Method not allowed' });
  }
}
