package ru.yandex.practicum.kafka.serializer;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.util.Map;

public class GeneralAvroSerializer implements Serializer<SpecificRecord> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public byte[] serialize(String topic, SpecificRecord data) {
        if (data == null) {
            return null;
        }

        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            Schema schema = data.getSchema();
            SpecificDatumWriter<SpecificRecord> datumWriter = new SpecificDatumWriter<>(schema);
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
            datumWriter.write(data, encoder);
            encoder.flush();
            return outputStream.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException("Не удалось сериализовать Avro-сообщение", e);
        }
    }

    @Override
    public void close() {
    }
}
