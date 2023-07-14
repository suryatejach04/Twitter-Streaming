import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
//import com.twitter.joauth.UnpackedRequest;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {
    private final static Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getSimpleName());

    public Client client;
    public KafkaProducer<String, String> producer;
    final BlockingQueue<String> allmsgs = new LinkedBlockingQueue<>(50);
    private List<String> trackTerms = Lists.newArrayList("covid");

    public static void main(String[] args) {
        new TwitterProducer().combine();
    }

    private void combine() {
        logger.info("starting setup");

        client = createTwitterClient(allmsgs);
        client.connect();

        Properties properties = new Properties();

        // connect to local host
        properties.setProperty("bootstrap.servers", ""127.0.0.1:9092"");

        //connect to conduktor platform
        properties.setProperty("bootstrap.servers", "cluster.playground.cdkt.io:9092");
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username='6C2s5A1LdCkyLNGH4sFaV7' password='eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiI2QzJzNUExTGRDa3lMTkdINHNGYVY3Iiwib3JnYW5pemF0aW9uSWQiOjc0NjQ1LCJ1c2VySWQiOjg2ODUzLCJmb3JFeHBpcmF0aW9uQ2hlY2siOiJmNjBmYzE3Ni0xNDRlLTQzYzgtYTk2MC02ZDYyNWE2NzhhNjAifX0.sG3ujeEaFXnOM-NXSk0LaZYEaFGHYmo6QCC3f1uljlg';");
        properties.setProperty("sasl.mechanism", "PLAIN");


        //set producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // create kafka-producer
        producer = new KafkaProducer<>(properties);

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                logger.info("Detected a shutdown, let's exit by calling consumer.wakeup()...");
                client.stop();
                logger.info("clossing producer");
                producer.close();
            }
        });
        String msg = null;
        while (!client.isDone()) {
            msg = null;
            try {
                msg = allmsgs.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
        }
        if (msg != null) {
            logger.info(msg);
            // create topic before sending data into it 
            // send messages to kafka through records 
            producer.send(new ProducerRecord<>("kafka-twitter", null, msg), new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception e) {
                    if (e != null) {
                        logger.error("some error is occured", e);
                    }
                }
            });
        }
        logger.info("Application ended");
    }

    // create twitter client
    private Client createTwitterClient(BlockingQueue<String> allmsgs) {

        Hosts twitterHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint twitterEndpoint = new StatusesFilterEndpoint();
        // words to be found out
        twitterEndpoint.trackTerms(Collections.singletonList(trackTerms.toString()));
       // authentication to Twitter api
        // create another class-TwitterConfig to store your keys and access keys from there
        Authentication twitterAuth = new OAuth1(TwitterConfig.CONSUMER_KEYS, TwitterConfig.CONSUMER_SECRETS, TwitterConfig.TOKEN, TwitterConfig.SECRET);
        ClientBuilder builder = new ClientBuilder()
                .name("twitter-client")
                .hosts(Constants.STREAM_HOST)
                .authentication(twitterAuth)
                .endpoint(twitterEndpoint)
                .processor(new StringDelimitedProcessor(allmsgs));
        Client twitterclient = builder.build();
        return twitterclient;
    }
}
