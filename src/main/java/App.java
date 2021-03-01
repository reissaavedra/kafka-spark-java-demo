//import org.apache.kafka.streams.KafkaStreams;
//import org.apache.kafka.streams.StreamsBuilder;
//import org.apache.kafka.streams.StreamsConfig;
//import org.apache.kafka.streams.kstream.Consumed;
//import org.apache.kafka.streams.kstream.KStream;
//import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import serde.AppSerdes;
import types.avro.PosInvoiceAvro;

import java.util.Properties;

public class App {
    static final Logger logger = LogManager.getLogger();

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, AppConfigs.appID);
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfigs.bootstrapServer);

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, PosInvoiceAvro> KS0 = streamsBuilder.stream(
                AppConfigs.inputTopicName,
                Consumed.with(AppSerdes.String(), AppSerdes.Invoice())
        );

        KS0.filter((k, v) ->
                v.getDeliveryType().toString().equalsIgnoreCase(AppConfigs.DELIVERY_TYPE_HOME_DELIVERY))
                .to(AppConfigs.outputTopicName, Produced.with(AppSerdes.String(), AppSerdes.Invoice()));

        KafkaStreams streams = new KafkaStreams(streamsBuilder.build(), properties);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            logger.info("Stopping Stream");
            streams.close();
        }));

    }
}
