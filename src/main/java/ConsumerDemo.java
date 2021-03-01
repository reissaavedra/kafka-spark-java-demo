import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemo {
    final Logger mLogger = LoggerFactory.getLogger(ConsumerDemo.class);
    private final String mBootstrapServer;
    private final String mGroupId;
    private final String mTopic;

    private Properties producerProps(String bootstrapServer){
        String serializer = StringSerializer.class.getName();
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, serializer);
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, serializer);
        return props;
    }

    ConsumerDemo(String bootstrapServer,String groupId, String topic) {
        mBootstrapServer = bootstrapServer;
        mGroupId = groupId;
        mTopic = topic;
    }

    private class ConsumerRunnable implements Runnable{
        private CountDownLatch mCountDownLatch;
        private KafkaConsumer<String,String> mConsumer;
        private Properties consumerProps(String bootstrapServer, String groupId){
            String deserializer = StringDeserializer.class.getName();
            Properties props = new Properties();
            props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
            props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, bootstrapServer);
            props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, deserializer);
            props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deserializer);
            props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            return props;
        }
        ConsumerRunnable(String bootstrapServer, String groupId, String topic, CountDownLatch latch){
            mCountDownLatch = latch;
            Properties props = consumerProps(bootstrapServer, groupId);
            mConsumer = new KafkaConsumer<>(props);
            mConsumer.subscribe(Collections.singletonList(topic));
        }

        @Override
        public void run() {
            try {
                do {
                    ConsumerRecords<String,String> records = mConsumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String,String> record: records){
                        mLogger.info("Key: "+ record.key()+ ", Value: "+record.value());
                        mLogger.info("Partition: "+record.partition()+", Offset: "+record.offset());
                    }
                }while (true);
            }catch (WakeupException e){
                mLogger.info("Received shutdown signal");
            }finally {
                mConsumer.close();
                mCountDownLatch.countDown();
            }
        }

        void shutdown(){
            mConsumer.wakeup();
        }
    }

    void run(){
        mLogger.info("Creating consumer thread");
        CountDownLatch latch = new CountDownLatch(1);

        ConsumerRunnable consumerRunnable = new ConsumerRunnable(mBootstrapServer, mGroupId, mTopic, latch);
        Thread thread = new Thread(consumerRunnable);
        thread.start();

        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            mLogger.info("Caught shutdown hook");
            consumerRunnable.shutdown();
            await(latch);

            mLogger.info("App exited");
        }));
    }

    void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            mLogger.error("Application got interrupted", e);
        } finally {
            mLogger.info("Application is closing");
        }
    }

    public static void main(String[] args) {
        String server = "127.0.0.1:9092";
        String groupId = "some_app";
        String topic = "input-topic";
        new ConsumerDemo(server,groupId,topic).run();
    }
}
