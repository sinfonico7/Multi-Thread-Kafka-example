package guru.learningjournal.kafka.examples;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class DispatcherDemo {
    public static final Logger logger = LogManager.getLogger();
    public  static void main(String[] args){

        Properties props = new Properties();
        InputStream inputStream;
        try {
            inputStream = new FileInputStream(AppConfigs.kafkaConfigFIleLocation);
            props.load(inputStream);
            props.put(ProducerConfig.CLIENT_ID_CONFIG,AppConfigs.applicationID);
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        } catch (IOException e) {
            e.printStackTrace();
        }

        KafkaProducer<Integer,String> producer = new KafkaProducer<Integer, String>(props);
        Thread[] dispatchers = new Thread[AppConfigs.eventFiles.length];
        logger.info("Starting threads!");
        for (int i = 0 ; i < AppConfigs.eventFiles.length ; i++){
            dispatchers[i] = new Thread(new Dispatcher(producer,AppConfigs.topicName,AppConfigs.eventFiles[i]));
            dispatchers[i].start();
        }
        try{
        for (Thread thread: dispatchers) thread.join();
            } catch (InterruptedException e) {
                logger.error("Threads are interrupted...");
                e.printStackTrace();
            }finally {
                producer.close();
                logger.info("Running threads are ready.");
            }


    }
}
