package data_generator;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import types.avro.DeliveryAddressAvro;
import types.avro.LineItemAvro;
import types.avro.PosInvoiceAvro;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class PosInvoiceGenerator {
    private static final Logger logger = LogManager.getLogger();
    private static PosInvoiceGenerator ourInstance = new PosInvoiceGenerator();
    private final Random invoiceIndex;
    private final Random invoiceNumber;
    private final Random numberOfItems;
    private final PosInvoiceAvro[] invoices;


    public static PosInvoiceGenerator getInstance() {
        return ourInstance;
    }

    private PosInvoiceGenerator() {
        String DATAFILE = "src/main/resources/data/Invoice.json";
        ObjectMapper mapper;
        invoiceIndex = new Random();
        invoiceNumber = new Random();
        numberOfItems = new Random();
        mapper = new ObjectMapper();
        try {
            invoices = mapper.readValue(new File(DATAFILE), PosInvoiceAvro[].class);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private int getIndex() {
        return invoiceIndex.nextInt(100);
    }

    private int getNewInvoiceNumber() {
        return invoiceNumber.nextInt(99999999) + 99999;
    }

    private int getNoOfItems() {
        return numberOfItems.nextInt(4) + 1;
    }

    public PosInvoiceAvro getNextInvoice() {
        PosInvoiceAvro invoice = invoices[getIndex()];
        invoice.setInvoiceNumber(Integer.toString(getNewInvoiceNumber()));
        invoice.setCreatedTime(System.currentTimeMillis());
        if ("HOME-DELIVERY".equalsIgnoreCase(invoice.getDeliveryType().toString())) {
            DeliveryAddressAvro deliveryAddress = AddressGenerator.getInstance().getNextAddress();
            invoice.setDeliveryAddress(deliveryAddress);
        }
        int itemCount = getNoOfItems();
        Double totalAmount = 0.0;
        List<LineItemAvro> items = new ArrayList<>();
        ProductGenerator productGenerator = ProductGenerator.getInstance();
        for (int i = 0; i < itemCount; i++) {
            LineItemAvro item = productGenerator.getNextProduct();
            totalAmount = totalAmount + item.getTotalValue();
            items.add(item);
        }
        invoice.setNumberOfItems(itemCount);
        invoice.setInvoiceLineItems(items);
        invoice.setTotalAmount(totalAmount);
        invoice.setTaxableAmount(totalAmount);
        invoice.setCGST(totalAmount * 0.025);
        invoice.setSGST(totalAmount * 0.025);
        invoice.setCESS(totalAmount * 0.00125);
        logger.debug(invoice);
        return invoice;
    }
}
