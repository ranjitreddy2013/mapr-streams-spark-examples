package com.spark.consumer;

/**
 * Created by ranjitlingaiah on 2/2/17.
 */
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import java.util.concurrent.ThreadLocalRandom;


public class HbasePutConvertFunction implements PairFunction<String, ImmutableBytesWritable, Put> {
    private final static String columnFamily = "cf";

    @Override
    public Tuple2<ImmutableBytesWritable, Put> call(String t) throws Exception {

        System.out.println("Record:" + t);
        Put put = new Put(Bytes.toBytes("rowkey" + ThreadLocalRandom.current().nextInt(1, 101)));
        put.add(Bytes.toBytes(columnFamily), Bytes.toBytes("z"),
                Bytes.toBytes(t));

        return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(), put);

    }
}