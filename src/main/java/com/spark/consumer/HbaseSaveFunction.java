package com.spark.consumer;

/**
 * Created by ranjitlingaiah on 2/2/17.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.streaming.Time;




public class HbaseSaveFunction implements VoidFunction2<JavaPairRDD<ImmutableBytesWritable, Put> , Time> {

    private Configuration jobConf = null;
//    private final static String tableName = "/user/ranjitlingaiah/readme";

    public HbaseSaveFunction(Configuration hbaseconf, Configuration jobConf) {

        //hbaseconf = HBaseConfiguration.create();
//        hbaseconf.set(TableOutputFormat.OUTPUT_TABLE, tableName);
        //jobConf  = new Configuration(hbaseconf);
        this.jobConf = jobConf;
//        jobConf.set("mapreduce.job.output.key.class", "Text");
//        jobConf.set("mapreduce.job.output.value.class", "Text");
//        jobConf.set("mapreduce.outputformat.class", "org.apache.hadoop.hbase.mapreduce.TableOutputFormat");
    }

    @Override
        public void call(JavaPairRDD<ImmutableBytesWritable, Put> immutableBytesWritablePutJavaPairRDD, Time time) throws Exception {

            immutableBytesWritablePutJavaPairRDD.saveAsNewAPIHadoopDataset(jobConf);
    }
}