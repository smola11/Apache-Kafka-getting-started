import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.Properties;

public class KafkaConsumerAssignApp {

    public static void main(String[] args) {

        // create properties dictionary for the required/optional Consumer config settings:
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092, localhost:9093, localhost:9094");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("group.id", "test");

        KafkaConsumer<String, String> myConsumer = new KafkaConsumer<>(properties);

        ArrayList<TopicPartition> partitions = new ArrayList<>();
        TopicPartition myTopicPartition0 = new TopicPartition("my-topic", 0);
        TopicPartition myOtherTopicPartition2 = new TopicPartition("myother--topic", 2);
        partitions.add(myTopicPartition0);
        partitions.add(myOtherTopicPartition2);

        myConsumer.assign(partitions);

        try {
            while (true) {
                ConsumerRecords<String, String> records = myConsumer.poll(10);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(String.format("Topic: %s, Partition %d, Offset: %d Key: %s, Value: %s",
                            record.topic(), record.partition(), record.offset(), record.key(), record.value()));
                }
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        } finally {
            myConsumer.close();
        }
    }
}
