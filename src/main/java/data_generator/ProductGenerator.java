package data_generator;

import com.fasterxml.jackson.databind.ObjectMapper;
import types.avro.LineItemAvro;

import java.io.File;
import java.util.Random;

public class ProductGenerator {
    private static final ProductGenerator ourInstance = new ProductGenerator();
    private final Random random;
    private final Random qty;
    private final LineItemAvro[] products;

    static ProductGenerator getInstance() {
        return ourInstance;
    }

    private ProductGenerator() {
        String DATAFILE = "src/main/resources/data/products.json";
        ObjectMapper mapper = new ObjectMapper();
        random = new Random();
        qty = new Random();
        try {
            products = mapper.readValue(new File(DATAFILE), LineItemAvro[].class);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private int getIndex() {
        return random.nextInt(100);
    }

    private int getQuantity() {
        return qty.nextInt(2) + 1;
    }

    LineItemAvro getNextProduct() {
        LineItemAvro lineItem = products[getIndex()];
        lineItem.setItemQty(getQuantity());
        lineItem.setTotalValue(lineItem.getItemPrice() * lineItem.getItemQty());
        return lineItem;
    }
}
