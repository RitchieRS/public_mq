/*
 *
 * =============================================================================
 *
 * Produce messages to Confluent Cloud
 * Using the node-rdkafka client for Apache Kafka
 *
 * =============================================================================
*/

const Kafka = require('node-rdkafka');
const { configFromCli } = require('./config');

const express = require('express')
const app = express();
const port = 3000;
var cors = require('cors');
const path = require('path');
const mysql = require('mysql');

var sql = new  mySqlvar();
sql.connect();

app.use(cors());
app.use(express.static('public'));
app.use(express.static('files'));
app.use(express.urlencoded({ extended: true }));
app.get('/', (req, res) => {
	res.send('Hello World!')
})

/*mysql*/
var mySqlvar =  require('./controller/MySqlController.js');

const ERR_TOPIC_ALREADY_EXISTS = 36;

function ensureTopicExists(config) {
  const adminClient = Kafka.AdminClient.create({
    'bootstrap.servers': config['bootstrap.servers'],
    'sasl.username': config['sasl.username'],
    'sasl.password': config['sasl.password'],
    'security.protocol': config['security.protocol'],
    'sasl.mechanisms': config['sasl.mechanisms']
  });

  return new Promise((resolve, reject) => {
    adminClient.createTopic({
      topic: config.topic,
      num_partitions: 1,
      replication_factor: 3
    }, (err) => {
      if (!err) {
        console.log(`Created topic ${config.topic}`);
        return resolve();
      }

      if (err.code === ERR_TOPIC_ALREADY_EXISTS) {
        return resolve();
      }

      return reject(err);
    });
  });
}

function createConsumer(config, onData) {
  const consumer = new Kafka.KafkaConsumer({
    'bootstrap.servers': config['bootstrap.servers'],
    'sasl.username': config['sasl.username'],
    'sasl.password': config['sasl.password'],
    'security.protocol': config['security.protocol'],
    'sasl.mechanisms': config['sasl.mechanisms'],
    'group.id': 'node-example-group-1'
  }, {
    'auto.offset.reset': 'earliest'
  });

  return new Promise((resolve, reject) => {
    consumer
      .on('ready', () => resolve(consumer))
      .on('data', onData);

    consumer.connect();
  });
}

function createProducer(config, onDeliveryReport) {
  const producer = new Kafka.Producer({
    'bootstrap.servers': config['bootstrap.servers'],
    'sasl.username': config['sasl.username'],
    'sasl.password': config['sasl.password'],
    'security.protocol': config['security.protocol'],
    'sasl.mechanisms': config['sasl.mechanisms'],
    'dr_msg_cb': true
  });

  return new Promise((resolve, reject) => {
    producer
      .on('ready', () => resolve(producer))
      .on('delivery-report', onDeliveryReport)
      .on('event.error', (err) => {
        console.warn('event.error', err);
        reject(err);
      });
    producer.connect();
  });
}

async function produceExample(message) {
  const config = await configFromCli();
  config.topic ='test1'
  if (config.usage) {
    return console.log(config.usage);
  }

  await ensureTopicExists(config);

  const producer = await createProducer(config, (err, report) => {
    if (err) {
      console.warn('Error producing', err)
    } else {
      const {topic, partition, value} = report;
      console.log(`Successfully produced record to topic "${topic}" partition ${partition} ${value}`);
    }
  });


    const key = 'roboot';
    const value = Buffer.from(JSON.stringify(message));

    console.log(`Producing record ${key}\t${value}`);

    producer.produce(config.topic, -1, value, key);


  producer.flush(10000, () => {
    producer.disconnect();
  });
}




  async function consumerExample() {
    const config = await configFromCli();
    config.topic ='test1'

    if (config.usage) {
      return config.usage;
    }

    console.log(`Consuming records from ${config.topic}`);

    let seen = 0;

    const consumer = await createConsumer(config, ({key, value, partition, offset}) => {
      console.log(`Consumed record with key ${key} and value ${value} of partition ${partition} @ offset ${offset}. Updated total count to ${++seen}`);
			return console.log(`${value}`);
    });

    consumer.subscribe([config.topic]);
    consumer.consume();

    process.on('SIGINT', () => {
      console.log('\nDisconnecting consumer ...');
      consumer.disconnect();
    });
  }



consumerExample().then(value=>{
	 console.log(value);
})
.catch((err) => {
console.error(`Something went wrong:\n${err}`);
process.exit(1);
});

app.post('/message-receptor', (req, res) => {

	var msg = {
		id:req.body.id,
		autor:req.body.autor,
		message:req.body.message
	}
	produceExample(req.body).then((value)=>{
						sql.saveMessage(msg,function(response){
							  res.json({message:"Successfully Published Message"});
						})
	       })
				.catch((err) => {
					console.error(`Something went wrong:\n${err}`);
					process.exit(1);
		});
			//res.json({status:"Successfully"});
})

app.listen(port, () => {
      console.log(`App listening at http://localhost:${port}`)
})
