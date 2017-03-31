each = require 'async/eachOf'
uuid = require 'uuid/v4'
Kafka = require 'node-rdkafka'

class KafkaProducers
  constructor: ({ brokers, topics }) ->
    topics = [topics] unless Array.isArray topics
    @producers = {}
    for topicName in topics # create a producer for each TOPIC
      producer = new Kafka.Producer {
        'metadata.broker.list': brokers
      }
      topic = producer.Topic topicName, {}
      producer.on 'error', (err) -> throw err
      @producers[topicName] =
        partition: null # leave partitioning logic to node-rdkafka
        producer: producer
        topic: topic


  connect: (callback) =>
    handlers = {}
    each @producers, ({ producer, topic, partition }, topicName, done) ->
      # initialize each PRODUCER, create Express 'post' handler for each one...
      producer.on 'ready', ->
        handlers[topicName] = (key_field_name, handler_cb) ->
          [key_field_name, handler_cb] = ['identifier', key_field_name] unless arguments.length > 1
          (req, res, next) ->
            data = res.locals?.body or req.body or {}
            user = res.locals?.user or req.user or {}
            data[key_field_name] = uuid()
            data.timestamp = new Date().toISOString()
            data.user = user
            try
              producer.produce topic, partition, Buffer.from(JSON.stringify data), data.identifier
              return handler_cb data, req, res, next if handler_cb?
              res.sendStatus 202
            catch ex
              next ex
        done()
      producer.connect (err, metadata) -> callback err, {} if err?
    , (err) ->
      callback err, handlers # return HANDLERS object keyed by TOPICNAME


  produce: (topicName, msg) => # produce single message to TOPIC
    return console.log "Topic name and msg payload are both required" unless arguments.length > 1
    return console.log "No topic #{topicName} found" unless @producers[topicName]?
    { producer, topic, partition } = @producers[topicName]
    try
      producer.produce topic, partition, Buffer.from(JSON.stringify msg), uuid()
    catch ex
      console.log ex


module.exports = KafkaProducers
