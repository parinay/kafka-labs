import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import java.util.Properties;

public class FilterHighValueTxns {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "txn-filter-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> txns = builder.stream("transactions");

        KStream<String, String> highValue = txns.filter((key, value) -> {
            try {
                double amount = Double.parseDouble(value.split("amount":")[1].split(",")[0]);
                return amount > 10000;
            } catch (Exception e) {
                return false;
            }
        });

        highValue.to("high_value_txns_streams");
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
    }
}