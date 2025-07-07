import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.kstream.Produced;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Properties;

public class FilterHighValueTxns {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "high-value-txn-filter");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        builder.<String, String>stream("transactions")
                .process(new ProcessorSupplier<String, String, String, String>() {
                    @Override
                    public Processor<String, String, String, String> get() {
                        return new HeaderFilteringProcessor();
                    }
                })
                .to("high_value_txns_streams", Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    public static class HeaderFilteringProcessor implements Processor<String, String, String, String> {

        private ProcessorContext<String, String> context;
        private final ObjectMapper mapper = new ObjectMapper();

        @Override
        public void init(ProcessorContext<String, String> context) {
            this.context = context;
        }

        @Override
        public void process(Record<String, String> record) {
            try {
                Headers headers = record.headers();
                var header = headers.lastHeader("event_type");
                String eventType = header != null ? new String(header.value()) : "";

                JsonNode node = mapper.readTree(record.value());
                double amount = node.get("amount").asDouble();
                String channel = node.get("channel").asText();

                if (amount > 10000 && "ATM".equals(channel) && "DEBIT".equals(eventType)) {
                    context.forward(record);
                }

            } catch (Exception e) {
                e.printStackTrace(); // optionally log
            }
        }

        @Override
        public void close() {
            // No-op
        }
    }
}