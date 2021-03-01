package data_generator;

import org.codehaus.jackson.map.ObjectMapper;
import types.LineItem;

import java.io.File;
import java.util.Random;

public class ProductGenerator {
    static final ProductGenerator instance = new ProductGenerator();
    final Random random;
    final Random qty;
    final LineItem[] products;

    static ProductGenerator getInstance() {
        return instance;
    }

    private ProductGenerator() {
        String DATAFILE = "/home/reisson/IdeaProjects/kafka-java/src/main/resources/data/products.json";
        ObjectMapper mapper = new ObjectMapper();
        random = new Random();
        qty = new Random();
        try{
            products = mapper.readValue(new File(DATAFILE), LineItem[].class);
        }catch(Exception e){
            throw new RuntimeException(e);
        }
    }

    private int getIndex(){
        return random.nextInt();
    }

    private int getQuantity() {
        return qty.nextInt(2) + 1;
    }

    LineItem getNextProduct() {
        LineItem lineItem = products[getIndex()];
        lineItem.setItemQty(getQuantity());
        lineItem.setTotalValue(lineItem.getItemPrice() * lineItem.getItemQty());
        return lineItem;
    }
}
