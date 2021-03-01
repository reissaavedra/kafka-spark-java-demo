package serde;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.codehaus.jackson.map.ObjectMapper;

import java.util.Map;

public class JsonDeserializer<T> implements Deserializer<T> {
    ObjectMapper mapper = new ObjectMapper();
    Class<T> className;
    static final String KEY_CLASS_NAME_CONFIG = "key.class.name";
    static final String VALUE_CLASS_NAME_CONFIG = "value.class.name";

    public JsonDeserializer() {}

    @Override
    public void configure(Map<String, ?> props, boolean isKey) {
        if(isKey) className = (Class<T>) props.get(KEY_CLASS_NAME_CONFIG);
        else className = (Class<T>) props.get(VALUE_CLASS_NAME_CONFIG);
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        if (data == null) return null;
        try {
            return mapper.readValue(data, className);
        }catch(Exception e){
            throw new SerializationException(e);
        }
    }

    @Override
    public void close() {

    }
}
