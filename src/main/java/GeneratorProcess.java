import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ser.std.StringSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import types.avro.PosInvoiceAvro;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class GeneratorProcess {
    static final Logger logger = LogManager.getLogger();

    public static void main(String[] args) {
        if (args.length < 3){
            System.out.println("Please provide command line arguments: topicName noOfProducers produceSpeed");
            System.exit(-1);
        }

        String topicName = args[0];
        int noOfProducers = Integer.valueOf(args[1]);
        int produceSpeed = Integer.valueOf(args[2]);

        Properties props = new Properties();
        props.put(ProducerConfig.CLIENT_ID_CONFIG, AppConfigs.appID);
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfigs.bootstrapServer);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);

        KafkaProducer<String, PosInvoiceAvro> kafkaProducer = new KafkaProducer<>(props);
        ExecutorService executor = Executors.newFixedThreadPool(noOfProducers);
        final List<RunnableProducer> runnableProducers = new ArrayList<>();

        for (int i = 0; i < noOfProducers; i++) {
            RunnableProducer runnableProducer = new RunnableProducer(i, kafkaProducer, topicName, produceSpeed);
            runnableProducers.add(runnableProducer);
            executor.submit(runnableProducer);
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            for (RunnableProducer p : runnableProducers) p.shutdown();
            executor.shutdown();
            logger.info("Closing Executor Service");
            try {
                executor.awaitTermination(produceSpeed * 2, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }));


    }
}
