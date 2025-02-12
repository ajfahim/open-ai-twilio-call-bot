import WebSocket from 'ws';

export const config = {
  runtime: 'nodejs18',
};

const SYSTEM_MESSAGE =
  "You are a helpful and bubbly AI assistant who loves to chat about anything the user is interested about and is prepared to offer them facts. You have a penchant for dad jokes, owl jokes, and rickrolling – subtly. Always stay positive, but work in a joke when appropriate.";
const VOICE = "alloy";

const LOG_EVENT_TYPES = [
  "error",
  "response.content.done",
  "rate_limits.updated",
  "response.done",
  "input_audio_buffer.committed",
  "input_audio_buffer.speech_stopped",
  "input_audio_buffer.speech_started",
  "session.created",
];

export default function handler(req, res) {
  if (req.method === 'GET') {
    const { Server } = WebSocket;
    const wss = new Server({ noServer: true });

    wss.on('connection', function connection(ws) {
      console.log('Client connected');
      let streamSid = null;
      let latestMediaTimestamp = 0;
      let lastAssistantItem = null;
      let markQueue = [];
      let responseStartTimestampTwilio = null;

      const openAiWs = new WebSocket(
        "wss://api.openai.com/v1/realtime?model=gpt-4o-realtime-preview-2024-10-01",
        {
          headers: {
            Authorization: `Bearer ${process.env.OPENAI_API_KEY}`,
            "OpenAI-Beta": "realtime=v1",
          },
        }
      );

      openAiWs.on('open', () => {
        console.log('Connected to OpenAI');
        const sessionUpdate = {
          type: "session.update",
          session: {
            turn_detection: { type: "server_vad" },
            input_audio_format: "g711_ulaw",
            output_audio_format: "g711_ulaw",
            voice: VOICE,
            instructions: SYSTEM_MESSAGE,
            modalities: ["text", "audio"],
            temperature: 0.8,
          },
        };
        openAiWs.send(JSON.stringify(sessionUpdate));
      });

      openAiWs.on('message', (data) => {
        try {
          const response = JSON.parse(data);
          
          if (LOG_EVENT_TYPES.includes(response.type)) {
            console.log(`Received event: ${response.type}`, response);
          }

          if (response.type === "response.audio.delta" && response.delta) {
            ws.send(JSON.stringify({
              event: "media",
              streamSid: streamSid,
              media: { payload: response.delta },
            }));

            if (response.item_id) {
              lastAssistantItem = response.item_id;
            }
          }

          if (response.type === "input_audio_buffer.speech_started") {
            if (markQueue.length > 0 && responseStartTimestampTwilio != null) {
              const elapsedTime = latestMediaTimestamp - responseStartTimestampTwilio;
              
              if (lastAssistantItem) {
                openAiWs.send(JSON.stringify({
                  type: "conversation.item.truncate",
                  item_id: lastAssistantItem,
                  content_index: 0,
                  audio_end_ms: elapsedTime,
                }));
              }
            }
          }
        } catch (error) {
          console.error('Error processing OpenAI message:', error);
        }
      });

      ws.on('message', function incoming(message) {
        try {
          const data = JSON.parse(message);
          if (data.event === "start") {
            streamSid = data.start.streamSid;
            console.log("Media WS: Received start event");
            responseStartTimestampTwilio = null;
            latestMediaTimestamp = 0;
          }

          if (data.event === "media") {
            latestMediaTimestamp = data.media.timestamp;
            if (openAiWs.readyState === WebSocket.OPEN) {
              openAiWs.send(JSON.stringify({
                type: "input_audio_buffer.append",
                audio: data.media.payload,
              }));
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

    // Handle the upgrade
    if (req.headers.upgrade && req.headers.upgrade.toLowerCase() === 'websocket') {
      wss.handleUpgrade(req, req.socket, Buffer.from(''), function done(ws) {
        wss.emit('connection', ws, req);
      });
    } else {
      res.status(426).json({ error: 'Upgrade Required' });
    }
  } else {
    res.status(405).json({ error: 'Method not allowed' });
  }
}
