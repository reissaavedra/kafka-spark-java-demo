package data_generator;

import com.fasterxml.jackson.databind.ObjectMapper;
import types.avro.DeliveryAddressAvro;

import java.io.File;
import java.util.Random;

public class AddressGenerator {
    private static final AddressGenerator ourInstance = new AddressGenerator();
    private final Random random;

    private DeliveryAddressAvro[] addresses;

    private int getIndex() {
        return random.nextInt(100);
    }

    static AddressGenerator getInstance() {
        return ourInstance;
    }

    private AddressGenerator() {
        final String DATAFILE = "src/main/resources/data/address.json";
        final ObjectMapper mapper;
        random = new Random();
        mapper = new ObjectMapper();
        try {
            addresses = mapper.readValue(new File(DATAFILE), DeliveryAddressAvro[].class);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    DeliveryAddressAvro getNextAddress() {
        return addresses[getIndex()];
    }
}
