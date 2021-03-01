import data_generator.PosInvoiceGenerator;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import types.avro.PosInvoiceAvro;

import java.util.concurrent.atomic.AtomicBoolean;

public class RunnableProducer implements Runnable{
    static final Logger logger = LogManager.getLogger();
    final AtomicBoolean stopper = new AtomicBoolean(false);
    KafkaProducer<String, PosInvoiceAvro> producer;
    String topicName ;
    PosInvoiceGenerator posInvoiceGenerator;
    int prodSpeed;
    int id;

    public RunnableProducer(int id,
                            KafkaProducer<String, PosInvoiceAvro> producer,
                            String topicName,
                            int prodSpeed) {
        this.producer = producer;
        this.id = id;
        this.prodSpeed = prodSpeed;
        this.topicName = topicName;
        this.posInvoiceGenerator = PosInvoiceGenerator.getInstance();
    }

    @Override
    public void run() {
        try {
            logger.info("Starting producer thread");
            PosInvoiceAvro posInvoice = posInvoiceGenerator.getNextInvoice();
            producer.send(new ProducerRecord<>(topicName, posInvoice.getStoreID().toString(), posInvoice));
            Thread.sleep(prodSpeed);
        }catch(Exception e){
            logger.error("Exception in Producer thread - " + id);
            throw new RuntimeException(e);
        }

    }

    void shutdown() {
        logger.info("Shutting down producer thread - " + id);
        stopper.set(true);
    }
}
