package com.spark.consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mapr.db.MapRDB;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka09.ConsumerStrategies;
import org.apache.spark.streaming.kafka09.KafkaUtils;
import org.apache.spark.streaming.kafka09.LocationStrategies;

import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;


public final class SparkConsumer {
    private static final ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        if (args.length < 5) {
            System.err.println("Usage: SparkConsumer <topics> <mapping_table_path> <data_warehouse_path> <duration> <master url>\n" +
                    "  <topics> is a list of one or more kafka topics to consume from\n" +
                    "  <mapping_table_path> is the table path that contains the mapping \n" +
                    "  <data_ware_house> is the location where the parquet files are stored\n" +
                    "  <duration> is the batch interval in seconds\n" +
                    "  <master url> is the master url\n\n");
            System.exit(1);
        }


        @SuppressWarnings("unchecked")

        List<Logger> loggers = Collections.<Logger>list(LogManager.getCurrentLoggers());
        loggers.add(LogManager.getRootLogger());
        for ( Logger logger : loggers ) {
            logger.setLevel(Level.OFF);
        }

        String topics = args[0];
        final String tablePath = args[1];  //e.g. "/user/ranjitlingaiah/devicetablemapping"
        final String dataWareHouseLocation = args[2];
        final String duration = args[3];
        String masterUrl = args[4];

        if (StringUtils.isEmpty(masterUrl)) {
            masterUrl = "local[2]";
        }

        // Create context with batch interval
        SparkConf sparkConf = new SparkConf().setAppName("SparkConsumer").setMaster(masterUrl);
        sparkConf.set("spark.sql.warehouse.dir", "/user/ranjitlingaiah/sw");

        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(Integer.parseInt(duration)));


        Set<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
        Map<String, Object> kafkaParams = new HashMap<>();
//        kafkaParams.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        kafkaParams.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, "mygroup");


        final String fastMessagesTablePath = "/user/ranjitlingaiah/iot-data";
        final  String tableName = "/user/ranjitlingaiah/readme";
        final  String columnFamily = "cf";


        System.out.println("Topics:" + topics);

//        JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(
//                jssc,
//                String.class,
//                String.class,
//                kafkaParams,
//                topicsSet
//        );


        final Pattern topicPattern = Pattern.compile(topics + ".*", Pattern.CASE_INSENSITIVE);

        JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(
                jssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, String>SubscribePattern(topicPattern,  kafkaParams));



        // Get the lines, split them into words, count the words and print
        JavaDStream<String> lines = messages.map(new Function<ConsumerRecord<String, String>, String>() {
            @Override
            public String call(ConsumerRecord<String, String> tuple2) {

                String s = tuple2.value();
                System.out.println("Incoming json:" + s);

                int i = 1;

                JsonNode incomingJson = null;
                JsonNode transformedJson = null;
                try {
                    incomingJson = mapper.readTree(s);
                    transformedJson = JsonUtil.transformJson(incomingJson, tablePath);

                }catch (IOException ioex) {
                    ioex.printStackTrace();
                }

                String outJson = null;
                try {
                    outJson = mapper.writeValueAsString(transformedJson);
                    System.out.println("Transformed JSON:" + outJson);
                }catch(Exception ex) {
                    ex.printStackTrace();
                }
                return outJson;
            }
        });


        final  Configuration hbaseconf = HBaseConfiguration.create();
        hbaseconf.set(TableOutputFormat.OUTPUT_TABLE, tableName);
        final Configuration jobConf  = new Configuration(hbaseconf);
        jobConf.set("mapreduce.job.output.key.class", "Text");
        jobConf.set("mapreduce.job.output.value.class", "Text");
        jobConf.set("mapreduce.outputformat.class", "org.apache.hadoop.hbase.mapreduce.TableOutputFormat");

        JavaPairDStream<ImmutableBytesWritable, Put> meterPairStream =  lines.mapToPair(new HbasePutConvertFunction());
        meterPairStream.foreachRDD(new HbaseSaveFunction(hbaseconf, jobConf));


        lines.foreachRDD(new VoidFunction2<JavaRDD<String>, Time>() {
            @Override
            public void call(JavaRDD<String> rdd, Time time) {
                SparkSession spark = JavaSparkSessionSingleton.getInstance(rdd.context().getConf());

                if (!rdd.isEmpty()) {
                    Dataset<Row> data = spark.read().json(rdd);
                    data.printSchema();
                    data.show();

                    data.repartition(1).write().mode(SaveMode.Append).partitionBy("table", "date").parquet(dataWareHouseLocation);
                }
            }
        });


        // Start the computation
        jssc.start();
        jssc.awaitTermination();
    }

    private static com.mapr.db.Table getTable(String tablePath) {

        if (!MapRDB.tableExists(tablePath)) {
            return MapRDB.createTable(tablePath);
        } else {
            return MapRDB.getTable(tablePath);
        }
    }
}