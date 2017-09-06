package net.rekod.app;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Map;

public class AvroDeserializer implements Deserializer<GenericRecord> {
    private Schema schema;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
      Schema.Parser parser = new Schema.Parser();
      try {
          schema = parser.parse(getClass().getResourceAsStream("/click.avsc"));
      } catch (IOException e) {
          throw new RuntimeException(e);
      }
    }

    @Override
    public GenericRecord deserialize(String topic, byte[] data) {
      DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
      Decoder decoder = DecoderFactory.get().binaryDecoder(data, null);

      try {
          GenericRecord record = reader.read(null, decoder);
          return record;
      } catch (Exception e) {
          throw new RuntimeException(e);
      }
    }

    @Override
    public void close() {

    }
}
