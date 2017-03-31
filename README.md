# kafka-simple-producer

Simple wrapper for `node-rdkafka` Producer class

## Installation

`npm i --save kafka-simple-producer`

The `node-rdkafka` compile requires that `libsasl2` libraries be installed on
your system.

## Build

`gulp clean && gulp build`

## Basic usage

```
express = require 'express'
bodyParser = require 'body-parser'

app = new express()
app.use bodyParser.json()
api = express.Router()

Producers = require 'kafka-simple-producer'
producers = new Producers { brokers: 'kafka:9092', topics: ['cat', 'vehicle'] }

producers.connect (err, handlers) -> # a HANDLER is created for each TOPIC passed to the constructor
  throw err if err?

  cat = express.Router()
  cat.post '/', handlers.cat()
  api.use '/cat', cat

  vehicle = express.Router()
  vehicle.get '/', (req, res, next) ->
    Vehicle.find().toArray().then (data) ->
      res.json data
    .catch (err) ->
      res.status(500).json err
  vehicle.post '/', handlers.vehicle()
  api.use '/vehicle', vehicle

  app.use '/', api

  port = process.env.PORT or 3000
  app.listen port, () ->
    console.log "App running on #{port}..."
```
