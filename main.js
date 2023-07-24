const { Amqp } = require('azure-iot-device-amqp');
const { Client } = require('azure-iot-device');
const connectionString = 'HostName=carteplus-iothub-nred.azure-devices.net;DeviceId=ckplcnred;SharedAccessKey=yE+SgpaLcuXFlNRy745eX/1pSOiw4jGro4sDCRoj/Tc=';
const client = Client.fromConnectionString(connectionString, Amqp);

const MONGODB_URI = 'mongodb://127.0.0.1:27017/';
const DATABASE_NAME = 'database';
const COLLECTION_NAME = 'table';

async function processData(data_json) {
  try {
    const client = await connectToMongoDB();
    const database = client.db(DATABASE_NAME);
    const collection = database.collection(COLLECTION_NAME);

    const result = await collection.insertOne(data_json);
    console.log('Inserted document:', result.ops[0]);

    client.close();
  } catch (err) {
    console.error('Error processing data:', err);
  }
}

function onMessageReceived(msg) {
  const messageData = msg.getData();
  try {
    const jsonString = messageData.toString('utf-8');
    const jsonData = JSON.parse(jsonString);
    console.log('Received Message (as JSON):');
    console.log(jsonData);
    processData(jsonData);


  } catch (error) {
    console.error('Received Message (not a valid JSON):');
    console.error(messageData.toString('utf-8'));
  }

  client.complete(msg, (err, result) => {
    if (err) {
      console.error('Error completing message:', err.toString());
    } else {
      console.log('Message marked as completed.');
    }
  });
}

function onError(err) {
  console.error('Error:', err.message);
}

client.open((err) => {
  if (err) {
    console.error('Error opening the connection:', err.message);
  } else {
    console.log('Connected to Azure IoT Hub.');

    client.on('message', onMessageReceived);
    client.on('error', onError);
  }
});

process.on('SIGINT', () => {
  console.log('Closing the connection...');
  client.close(() => {
    console.log('Connection closed.');
    process.exit(0);
  });
});
