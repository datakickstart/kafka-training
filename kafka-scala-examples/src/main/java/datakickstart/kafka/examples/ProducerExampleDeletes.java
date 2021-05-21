/*
    Based on example at https://www.programcreek.com/java-api-examples/?code=uber%2FuReplicator%2FuReplicator-master%2FuReplicator-Worker-3.0%2Fsrc%2Ftest%2Fjava%2Fcom%2Fuber%2Fstream%2Fureplicator%2Fworker%2FTestUtils.java
 */
package datakickstart.kafka.examples;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.util.Properties;

public class ProducerExampleDeletes {

    private static KafkaProducer createProducer(String bootstrapServer) {
        Properties producerProps = new Properties();
        producerProps
                .setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        producerProps.setProperty(ProducerConfig.CLIENT_ID_CONFIG, "test");
        producerProps.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                ByteArraySerializer.class.getName());
        producerProps.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                ByteArraySerializer.class.getName());
        KafkaProducer producer = new KafkaProducer(producerProps);
        return producer;
    }

    public static void produceMessages(String bootstrapServer, String topicname, int messageCount) {
        KafkaProducer producer = createProducer(bootstrapServer);
        for (int i = 0; i < messageCount; i++) {
//            ProducerRecord<Byte[], Byte[]> record = new ProducerRecord(topicname, null,
//                    String.format("Test Value - %d", i).getBytes());
            ProducerRecord<Byte[], Byte[]> record = new ProducerRecord(topicname, "4".getBytes(), null);
            producer.send(record);
        }
        producer.flush();
        producer.close();
    }

    public static void main(final String[] args) {
        produceMessages("0.0.0.0:9092", "subscriber-usage-demo", 1);
    }
}
