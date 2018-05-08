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
      topic = Kafka.Topic topicName
      producer.on 'error', (err) -> throw err
      @producers[topicName] =
        partition: null # leave partitioning logic to node-rdkafka
        producer: producer
        topic: topic

  connect: (callback) =>
    handlers = {}
    middleware = {}
    each @producers, ({ producer, topic, partition }, topicName, done) =>
      # initialize each PRODUCER, create Express handler for each one...
      producer.on 'event.error', callback
      producer.on 'ready', =>
        mw = @_generic_mw producer, topic, partition
        handlers[topicName] = [mw, @_generic_response]
        middleware[topicName] = mw
        done()
      producer.connect()
    , (err) ->
      callback err, handlers, middleware

  _generic_mw: (producer, topic, partition) ->
    @_with_message (msg, req, res, next) ->
      try
        producer.produce topic, partition, Buffer.from(JSON.stringify msg), msg.key
        next()
      catch ex
        next ex

  _generic_response: (req, res) ->
    res.sendStatus 202

  produce: (topicName, msg, key=uuid()) => # produce single message to TOPIC
    return console.log "Topic name and msg payload are both required" unless arguments.length > 1
    return console.log "No topic #{topicName} found" unless @producers[topicName]?
    { producer, topic, partition } = @producers[topicName]
    try
      producer.produce topic, partition, Buffer.from(JSON.stringify msg), key
    catch ex
      console.log ex

  _with_message: (callback) ->
    (req, res, next) ->
      msg = {}
      msg.http_method = req.method.toUpperCase()
      msg.key = uuid()
      msg.timestamp = new Date().toISOString()
      msg.user = res.locals?.user or req.user or {}
      msg.payload = res.locals?.body or req.body or {}
      res.locals.message = msg # make MSG available to subsequent middleware
      callback msg, req, res, next


module.exports = KafkaProducers
