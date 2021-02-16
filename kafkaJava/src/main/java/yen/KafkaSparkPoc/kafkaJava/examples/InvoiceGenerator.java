package yen.KafkaSparkPoc.kafkaJava.examples;

// https://github.com/yennanliu/Apache-Kafka-For-Absolute-Beginners/blob/master/07-pos-simulator-completed/src/main/java/guru/learningjournal/kafka/examples/datagenerator/InvoiceGenerator.java

import com.fasterxml.jackson.databind.ObjectMapper;
import yen.KafkaSparkPoc.kafkaJava.examples.types.DeliveryAddress;
import yen.KafkaSparkPoc.kafkaJava.examples.types.PosInvoice;
import yen.KafkaSparkPoc.kafkaJava.examples.types.LineItem;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class InvoiceGenerator {
    private static final Logger logger = LogManager.getLogger();
    private static InvoiceGenerator ourInstance = new InvoiceGenerator();
    private final Random invoiceIndex;
    private final Random invoiceNumber;
    private final Random numberOfItems;
    private final PosInvoice[] invoices;

    public static InvoiceGenerator getInstance(){
        return ourInstance;
    }

    private InvoiceGenerator(){
        String DATAFILE = "src/main/resources/data/Invoice.json";
        ObjectMapper mapper;
        invoiceIndex = new Random();
        invoiceNumber = new Random();
        numberOfItems = new Random();
        mapper = new ObjectMapper();

        try {
            invoices = mapper.readValue(new File(DATAFILE), PosInvoice[].class);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private int getIndex(){
        return invoiceIndex.nextInt(100);
    }

    private int getNewInvoiceNumber() {
        return invoiceNumber.nextInt(99999999) + 99999;
    }

    private int getNoOfItems() {
        return numberOfItems.nextInt(4) + 1;
    }

    public PosInvoice getNextInvoice() {
        PosInvoice invoice = invoices[getIndex()];
        invoice.setInvoiceNumber(Integer.toString(getNewInvoiceNumber()));
        invoice.setCreatedTime(System.currentTimeMillis());
        if ("HOME-DELIVERY".equalsIgnoreCase(invoice.getDeliveryType())) {

            // TO IMPLEMENT : AddressGenerator
            DeliveryAddress deliveryAddress = AddressGenerator.getInstance().getNextAddress();
            invoice.setDeliveryAddress(deliveryAddress);
        }
        int itemCount = getNoOfItems();
        Double totalAmount = 0.0;
        List<LineItem> items = new ArrayList<>();

        // TO IMPLEMENT : ProductGenerator
        ProductGenerator productGenerator = ProductGenerator.getInstance();

        for (int i = 0; i < itemCount; i++){
            LineItem item = ProductGenerator.getNextProduct();
            totalAmount += item.getTotalValue();
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
