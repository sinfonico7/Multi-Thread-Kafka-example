package guru.learningjournal.kafka.examples;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

public class Dispatcher implements Runnable{
    public static final Logger logger = LogManager.getLogger();
    private String fileLocation;
    private String topicName;
    private KafkaProducer<Integer,String> producer;

    Dispatcher(KafkaProducer<Integer,String> producer,String topicName,String fileLocation){
        this.fileLocation = fileLocation;
        this.producer = producer;
        this.topicName = topicName;
    }
    int counter = 0;
    @Override
    public void run() {
        logger.info("Reading the file " + fileLocation);
        File file = new File(fileLocation);
        try (Scanner scanner = new Scanner(file)){
            while (scanner.hasNext()){
                String line = scanner.nextLine();
                producer.send(new ProducerRecord<>(AppConfigs.topicName,null,line));
                counter++;
            }
            logger.info("Finished thread with " + counter +" messages from "+ fileLocation);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }
}
