package serde;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import types.Invoice;

import java.util.HashMap;
import java.util.Map;

public class AppSerdes extends Serdes{
    static final class InvoiceSerde extends Serdes.WrapperSerde<Invoice>{
        InvoiceSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>());
        }
    }

    public static Serde<Invoice> Invoice(){
        InvoiceSerde serde = new InvoiceSerde();

        Map<String, Object> serdeConf = new HashMap<>();
        serdeConf.put(JsonDeserializer.VALUE_CLASS_NAME_CONFIG, Invoice.class);
        serde.configure(serdeConf, false);

        return serde;
    }
}
