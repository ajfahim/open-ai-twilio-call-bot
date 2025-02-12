import dotenv from "dotenv";
import express from "express";
import cors from "cors";
import twilio from "twilio";
import { WebSocketServer } from "ws";
import WebSocket from "ws";

dotenv.config();

const app = express();
app.use(cors());
app.use(express.json());

// Constants
const SYSTEM_MESSAGE =
  "You are a helpful and bubbly AI assistant who loves to chat about anything the user is interested about and is prepared to offer them facts.";
const VOICE = "alloy";
const PORT = process.env.PORT || 5050;

// Root Route
app.get("/", (req, res) => {
  res.json({ message: "Twilio Media Stream Server is running!" });
});

// Route for making outgoing calls
app.post("/make-call", async (req, res) => {
  try {
    const { phoneNumber, voice = VOICE, prompt = SYSTEM_MESSAGE } = req.body;
    const client = twilio(process.env.TWILIO_ACCOUNT_SID, process.env.TWILIO_AUTH_TOKEN);

    const call = await client.calls.create({
      to: phoneNumber,
      from: process.env.PHONE_NUMBER_FROM,
      url: `${process.env.DOMAIN}/incoming-call?voice=${voice}&prompt=${encodeURIComponent(prompt)}`,
    });

    res.json({ success: true, callSid: call.sid });
  } catch (error) {
    console.error('Error making outgoing call:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// Route for Twilio to handle incoming calls
app.all("/incoming-call", (req, res) => {
  const voice = req.query.voice || VOICE;
  const prompt = req.query.prompt || SYSTEM_MESSAGE;
  
  const twimlResponse = `<?xml version="1.0" encoding="UTF-8"?>
    <Response>
      <Say>Please wait while we connect your call to the A. I. voice assistant.</Say>
      <Pause length="1"/>
      <Say>O.K. you can start talking!</Say>
      <Connect>
        <Stream url="wss://${req.headers.host}/api/websocket" />
      </Connect>
    </Response>`;

  res.type("text/xml").send(twimlResponse);
});

// Serve static files
app.use(express.static('.'));

// Handle WebSocket in development
if (!process.env.VERCEL) {
  const server = app.listen(PORT, () => {
    console.log(`Server is running on port ${PORT}`);
  });

  const wss = new WebSocketServer({ server });

  wss.on("connection", function connection(ws) {
    console.log("Client connected");
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

    openAiWs.on("open", () => {
      console.log("Connected to the OpenAI Realtime API");
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

    openAiWs.on("message", (data) => {
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
        console.error("Error processing OpenAI message:", error);
      }
    });

    ws.on("message", (msg) => {
      try {
        const data = JSON.parse(msg);
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
        console.error("Error processing Twilio message:", error);
      }
    });

    ws.on("close", () => {
      console.log("Client disconnected");
      if (openAiWs.readyState === WebSocket.OPEN) {
        openAiWs.close();
      }
    });
  });
}

export default app;
