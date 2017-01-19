package com.spark.consumer;

/**
 * Created by ranjitlingaiah on 1/16/17.
 */

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

/** Lazily instantiated singleton instance of SparkSession */
class JavaSparkSessionSingleton {
    private static transient SparkSession instance = null;
    public static SparkSession getInstance(SparkConf sparkConf) {
        if (instance == null) {
            instance = SparkSession
                    .builder()
                    .config(sparkConf)
                    .getOrCreate();
        }
        return instance;
    }
}
