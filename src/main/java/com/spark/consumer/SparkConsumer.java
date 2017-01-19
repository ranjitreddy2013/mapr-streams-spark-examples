package com.spark.consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.v09.KafkaUtils;
import scala.Tuple2;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

import static com.spark.consumer.Constants.DW_DIR_NAME;
import static com.spark.consumer.Constants.DW_LOCATION;


public final class SparkConsumer {
    private static final ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: SparkConsumer <topics>\n" +
                    "  <topics> is a list of one or more kafka topics to consume from\n\n");
            System.exit(1);
        }

        String topics = args[0];

        // Create context with a 2 seconds batch interval
        SparkConf sparkConf = new SparkConf().setAppName("JavaDirectKafkaWordCount").setMaster("local[2]").set("spark.executor.memory","1g");

        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(2));

        Set<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        System.out.println("Topics:" + topics);

        JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(
                jssc,
                String.class,
                String.class,
                kafkaParams,
                topicsSet
        );


        messages.print();

        // Get the lines, split them into words, count the words and print
        JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
            @Override
            public String call(Tuple2<String, String> tuple2) {

                String s = tuple2._2();
                System.out.println("Before updating the json:" + s);

                JsonNode incomingJson = null;
                JsonNode transformedJson = null;
                try {
                    incomingJson = mapper.readTree(s);
                    transformedJson = JsonUtil.transformJson(incomingJson);

                }catch (IOException ioex) {
                    ioex.printStackTrace();
                }

                String outJson = null;
                try {
                    outJson = mapper.writeValueAsString(transformedJson);
                    System.out.println("Transformed:" + outJson);
                }catch(Exception ex) {
                    ex.printStackTrace();
                }
                return outJson;
            }
        });



        lines.foreachRDD(new VoidFunction2<JavaRDD<String>, Time>() {
            @Override
            public void call(JavaRDD<String> rdd, Time time) {
                SparkSession spark = JavaSparkSessionSingleton.getInstance(rdd.context().getConf());
                System.out.println("Call...");

                if (!rdd.isEmpty()) {
                    Dataset<Row> data = spark.read().json(rdd);
                    data.printSchema();
                    data.show();

                    data.repartition(1).write().partitionBy("table", "date").parquet(DW_LOCATION + getDateString() + DW_DIR_NAME);
                }
            }
        });


        // Start the computation
        jssc.start();
        jssc.awaitTermination();
    }

    private static String getDateString()  {
        SimpleDateFormat sd = new SimpleDateFormat(Constants.PARTITION_PATTERN);
        return sd.format(new Date());
    }


}