const mqtt = require('mqtt');
const express = require('express');
const bodyParser = require('body-parser');
const WebSocket = require('ws');
const fs = require('fs').promises;
const { PubSub } = require('@google-cloud/pubsub');
const pubSubClient = new PubSub();

// Setup Express server
const app = express();
const port = 3001;  // Choose a port for your API server
app.use(bodyParser.json());

// MQTT Broker settings (replace with your broker info)
const mqttBroker = 'mqtt://test.mosquitto.org';  // Broker URL (can be local or cloud-based)
const mqttTopic = 'weather/station';  // Topic for receiving sensor data

// Google Cloud Pub/Sub setup
const topicName = 'weather-data-topic'; // Use environment variable or default
const subscriptionName = 'weather-data-subscription'; // Subscription for receiving messages

// Store the latest Pub/Sub message
let latestPubSubMessage = null; // Initialize with null



// Function to publish data to Google Cloud Pub/Sub
async function publishToPubSub(message) {
  const dataBuffer = Buffer.from(message);
  const topic = pubSubClient.topic(topicName);
  try {
    const messageId = await topic.publish(dataBuffer);
    console.log(`Message ${messageId} published to Pub/Sub`);
  } catch (err) {
    console.error('Error publishing to Pub/Sub:', err.message);
    console.error('Full error details:', err);
  }
}

// MQTT Client setup
const client = mqtt.connect(mqttBroker);

// Connect to the MQTT broker
client.on('connect', () => {
  console.log('Connected to MQTT broker');
  client.subscribe(mqttTopic, (err) => {
    if (!err) {
      console.log(`Subscribed to topic: ${mqttTopic}`);
    }
  });
});

// Handle incoming messages from MQTT and save data locally, in memory, and to Pub/Sub
client.on('message', async (topic, message) => {
  if (topic === mqttTopic) {
    try {
      const data = message.toString();
      console.log('Received data:', data);

      // Save to JSON file (persist data locally)
      await fs.writeFile('weatherData.json', JSON.stringify({ data: data, timestamp: new Date() }));
      console.log('Data saved to file');

      // Store in memory (for real-time updates)
      global.weatherData = data;

      // Publish the data to Google Cloud Pub/Sub
      publishToPubSub(data);
    } catch (err) {
      console.error('Error handling MQTT message:', err.message);
    }
  }
});
app.get('/', (req, res) => {
  res.send('Hello, Vercel!');
});

// API endpoint to retrieve the latest weather data
app.get('/api/weather/current', (req, res) => {
  try {
    if (global.weatherData) {
      res.json({ status: 'success', data: global.weatherData });
    } else {
      res.json({ status: 'error', message: 'No data available' });
    }
  } catch (err) {
    console.error('Error handling request:', err);
    res.status(500).json({ status: 'error', message: 'Internal Server Error' });
  }
});

// API endpoint to retrieve the latest Pub/Sub message
app.get('/api/pubsub/messages', (req, res) => {
  if (latestPubSubMessage) {
    res.json({
      status: 'success',
      data: latestPubSubMessage,
    });
  } else {
    res.json({
      status: 'error',
      message: 'No messages available',
    });
  }
});

// Listen for incoming Pub/Sub messages and store the latest one
function listenForMessages() {
  const subscription = pubSubClient.subscription(subscriptionName);

  subscription.on('message', (message) => {
    console.log(`Received Pub/Sub message: ${message.data.toString()}`);
    
    // Store only the latest message
    latestPubSubMessage = {
      id: message.id,
      data: message.data.toString(),
      attributes: message.attributes,
      timestamp: message.publishTime,
    };

    // Acknowledge the message
    message.ack();
  });

  subscription.on('error', (error) => {
    console.error(`Subscriber error: ${error.message}`);
  });

  console.log(`Listening for messages on subscription: ${subscriptionName}`);
}

// Start listening for messages
listenForMessages();

// Setup WebSocket server for real-time data on a separate port
const wsPort = 3002;  // Use a different port for WebSocket server
const wss = new WebSocket.Server({ port: wsPort });

// Handle WebSocket connections
wss.on('connection', (ws) => {
  console.log('Client connected to WebSocket');

  // Send the latest weather data to the client
  if (global.weatherData) {
    ws.send(JSON.stringify({ status: 'success', data: global.weatherData }));
  }
});

// Broadcast new data to all connected WebSocket clients
client.on('message', (topic, message) => {
  if (topic === mqttTopic) {
    const data = message.toString();
    global.weatherData = data;

    // Broadcast to all connected WebSocket clients
    wss.clients.forEach((client) => {
      if (client.readyState === WebSocket.OPEN) {
        client.send(JSON.stringify({ status: 'success', data: global.weatherData }));
      }
    });
  }
});

// Start the Express server on port 3001
app.listen(port, () => {
  console.log(`Backend server is running on http://localhost:${port}`);
});

// Start WebSocket server on port 3002
console.log(`WebSocket server running on ws://localhost:${wsPort}`);

// Graceful shutdown
process.on('SIGINT', () => {
  client.end();
  wss.close();
  console.log('Shutting down gracefully...');
  process.exit();
});
