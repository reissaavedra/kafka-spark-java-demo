package data_generator;

import org.codehaus.jackson.map.ObjectMapper;
import types.DeliveryAddress;

import java.io.File;
import java.util.Random;

public class AddressGenerator {
    static final AddressGenerator instance = new AddressGenerator();
    final Random random;
    private DeliveryAddress[] addresses;


    private int getIndex(){
        return random.nextInt(100);
    }

    static AddressGenerator getInstance() {
        return instance;
    }

    private AddressGenerator() {
        final String DATAFILE = "/home/reisson/IdeaProjects/kafka-java/src/main/resources/data/address.json";
        final ObjectMapper mapper;
        random = new Random();
        mapper = new ObjectMapper();

        try {
            addresses = mapper.readValue(new File(DATAFILE), DeliveryAddress[].class);
        }catch(Exception e){
            throw new RuntimeException(e);
        }
    }

    DeliveryAddress getNextAddress(){
        return addresses[getIndex()];
    }
}
