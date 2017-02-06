package com.spark.consumer;

/**
 * Created by ranjitlingaiah on 1/31/17.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapred.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;


public class SparkToHBase {

    private final static String tableName = "/user/ranjitlingaiah/readme";
    private final static String columnFamily = "cf";

    private static JavaSparkContext sc;

    public static void main(String[] args) throws Exception {
        String mode = "local[2]";
        sc = new JavaSparkContext(mode, "Write HBase data");

        Configuration conf = HBaseConfiguration.create();
        // new Hadoop API configuration
        Job newAPIJobConfiguration = Job.getInstance(conf);
        newAPIJobConfiguration.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, tableName);
        newAPIJobConfiguration.setOutputFormatClass(org.apache.hadoop.hbase.mapreduce.TableOutputFormat.class);

        // old Hadoop API configuration
        JobConf oldAPIJobConfiguration = new JobConf(conf, SparkToHBase.class);
        oldAPIJobConfiguration.setOutputFormat(TableOutputFormat.class);
        oldAPIJobConfiguration.set(TableOutputFormat.OUTPUT_TABLE, tableName);


        HBaseAdmin hBaseAdmin = null;
        try {
            hBaseAdmin = new HBaseAdmin(conf);
            if (hBaseAdmin.isTableAvailable(tableName)) {
                System.out.println("Table " + tableName + " is available.");
            } else {
                System.out.println("Table " + tableName + " is not available.");
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } finally {
            hBaseAdmin.close();
        }


        System.out.println("-----------------------------------------------");
        writeRowNewHadoopAPI(newAPIJobConfiguration.getConfiguration(), mode);
        System.out.println("-----------------------------------------------");
        sc.stop();

    }


    private static void writeRowNewHadoopAPI(Configuration conf, String mode) {

        JavaRDD<String> records = sc.textFile("README1", 1);

        JavaPairRDD<ImmutableBytesWritable, Put> hbasePuts = records.mapToPair(new PairFunction<String, ImmutableBytesWritable, Put>() {
            @Override
            public Tuple2<ImmutableBytesWritable, Put> call(String t)
                    throws Exception {

                System.out.println("Record:" + t);

                Put put = new Put(Bytes.toBytes("rowkey" + ThreadLocalRandom.current().nextInt(1, 101)));
                put.add(Bytes.toBytes(columnFamily), Bytes.toBytes("z"),
                        Bytes.toBytes(t));

                return new Tuple2<ImmutableBytesWritable, Put>(
                        new ImmutableBytesWritable(), put);
            }
        });

        hbasePuts.saveAsNewAPIHadoopDataset(conf);

    }

}