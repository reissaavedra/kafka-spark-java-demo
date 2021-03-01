package serde;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;
import org.codehaus.jackson.map.ObjectMapper;

import java.util.Map;

public class JsonSerializer<T> implements Serializer<T> {
    private final ObjectMapper mapper = new ObjectMapper();
    public JsonSerializer(){}


    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public byte[] serialize(String topic, T data) {
        if (data == null){
            return null;
        }
        try {
            return mapper.writeValueAsBytes(data);
        }catch(Exception e){
            throw new SerializationException("Error serializing JSON message: ",e);
        }
    }

    @Override
    public byte[] serialize(String topic, Headers headers, T data) {
        return new byte[0];
    }

    @Override
    public void close() {

    }
}
