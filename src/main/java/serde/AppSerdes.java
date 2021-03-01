package serde;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import types.avro.PosInvoiceAvro;

import java.util.HashMap;
import java.util.Map;

public class AppSerdes extends Serdes{
//    static final class InvoiceSerde extends Serdes.WrapperSerde<Invoice>{
//        InvoiceSerde() {
//            super(new JsonSerializer<>(), new JsonDeserializer<>());
//        }
//    }

//    public static Serde<Invoice> Invoice(){
//        InvoiceSerde serde = new InvoiceSerde();
//        Map<String, Object> serdeConf = new HashMap<>();
//        serdeConf.put(JsonDeserializer.VALUE_CLASS_NAME_CONFIG, Invoice.class);
//        serde.configure(serdeConf, false);
//        return serde;
//    }

    public static Serde<PosInvoiceAvro> Invoice(){
        Serde<PosInvoiceAvro> serde = new SpecificAvroSerde<>();
        Map<String,Object> serdeConf = new HashMap<>();
        serdeConf.put("schema.registry.url", "http://localhost:8081");
        serde.configure(serdeConf,false);
        return serde;
    }
}
