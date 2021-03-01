import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemo {
    final KafkaProducer<String,String> mProducer;
    final Logger mLogger = LoggerFactory.getLogger(ProducerDemo.class);
    private Properties producerProps(String bootstrapServer){
        String serializer = StringSerializer.class.getName();
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, serializer);
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, serializer);
        return props;
    }

    public ProducerDemo(String bootstrapServer) {
        Properties props = producerProps(bootstrapServer);
        mProducer = new KafkaProducer<>(props);
        mLogger.info("Producer initializer");
    }

    void put(String topic,String key, String value) throws ExecutionException,InterruptedException{
        mLogger.info("Put value: " + value +", for key: "+ key);

        ProducerRecord<String,String> record = new ProducerRecord<>(topic,key,value);
        mProducer.send(record, ((recordMetadata, e) -> {
            if(e!=null){
                mLogger.error("Error while producing", e);
                return;
            }
            mLogger.info("Received metadata.");
        }));
    }

    void close(){
        mLogger.info("Closing producer");
        mProducer.close();
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        String server = "127.0.0.1:9092";
        String topic = "input-topic";
        ProducerDemo producer = new ProducerDemo(server);
        producer.put(topic,"usr1","John");
        producer.put(topic,"usr2","Peter");
        producer.close();
    }
}
