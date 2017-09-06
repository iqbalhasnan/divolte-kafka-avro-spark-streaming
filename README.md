# Divolte Kafka Avro Spark Streaming Example

A working example of deserializing Avro schema data that being sent to Kafka from Divolte.

## Compile

```shell
mvn package
```

## Run Spark Streaming

```shell
spark-submit --master "local[2]" target/divolte-kafka-avro-spark-streaming-1.0-SNAPSHOT-jar-with-dependencies.jar 100
```

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/iqbalhasnan/divolte-kafka-avro-spark-streaming. This project is intended to be a safe, welcoming space for collaboration, and contributors are expected to adhere to the [Contributor Covenant](http://contributor-covenant.org) code of conduct.

## License

The project is available as open source under the terms of the [MIT License](http://opensource.org/licenses/MIT).
