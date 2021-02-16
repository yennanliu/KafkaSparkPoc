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

}
