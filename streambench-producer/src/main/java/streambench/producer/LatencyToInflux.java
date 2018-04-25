package streambench.producer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import streambench.workload.pojo.WorkloadConfig;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class LatencyToInflux {

    private InfluxDB influxDB;
    private KafkaConsumer<String, String> consumer;

    private String influxServer;
    private String db;
    private List<String> kafkaBrokers;
    private WorkloadConfig workloadConfig;

    private long batchSize;

    public LatencyToInflux(List<String> kafkaBrokers, WorkloadConfig workloadConfig,
                           String influxServer, String db, ScheduledExecutorService executorService) {
        this.kafkaBrokers = kafkaBrokers;
        this.workloadConfig = workloadConfig;
        this.influxServer = influxServer;
        this.db = db;

        connectToInflux();
        connectToKafka();

        executorService.schedule(new MsgReader(), 2, TimeUnit.SECONDS);
        executorService.scheduleAtFixedRate(new InfluxFlusher(), 5, 5, TimeUnit.SECONDS);
    }

    private void connectToInflux() {
        influxDB = InfluxDBFactory.connect("http://" + influxServer);
        influxDB.enableBatch();

        if(!influxDB.databaseExists(db)) {
            influxDB.createDatabase(db);
        }
        System.out.println("Connected to " + influxServer);
    }

    private void connectToKafka() {
        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaBrokers);
//        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer<>(props);

        consumer.subscribe(workloadConfig.getSinks());
        System.out.println("Subscribed Kafka to " + workloadConfig.getSinks());
    }

    class MsgReader implements Runnable {
        @Override
        public void run() {
            while (true) {
                BatchPoints points = BatchPoints.database(db).build();

                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    String ts = record.value().split(",")[0];
                    Long latency = System.nanoTime() - new Long(ts);

                    Point point = Point
                            .measurement("latency")
                            .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
                            .addField("value", latency)
                            .build();
                    points.point(point);
                }

                try {
                    influxDB.write(points);
                } catch (Exception e) {
                    System.err.println("Could not write to InfluxDB");
                    e.printStackTrace();
                }

                batchSize += points.getPoints().size();
            }
        }
    }

    class InfluxFlusher implements Runnable {
        @Override
        public void run() {
            if(influxDB != null) {
                System.out.println("Flushing " + batchSize + " points to InfluxDB");
                batchSize = 0;
                influxDB.flush();
            }
        }
    }
}
