package com.spark.consumer;

public class Constants {
    public static final String ENRICHED_FIELDS = "enriched_fields";
    public static final String PARSED_FIELDS = "parsed_fields";
    public static final String PARTITION_DATE_PATTERN = "dd-MM-yyyy";
    public static final String INCOMING_TIME_PATTERN = "yyyy-MM-dd HH:mm:ss";

    //Incoming JSON fields that used to partition data
    public static final String DEVICE_TYPE_IN_JSON = "device_type";
    public static final String TIME_IN_JSON = "time";

    //Outgoing JSON that is stored as Parquet will contain following two fields that will be used to partition the data
    public static final String DATE_OUT_JSON = "date"; //Uses field "time" in incoming json and extracts the date.
    public static final String TABLE_OUT_JSON = "table"; // created based on the table mapping of "device_type" field in MapR-DB

    //MapR-DB constants
    // Table is created in advance with below column family and column name.  Table name is provided to the Consumer.
    public static final String CF = "cf";        // Column family
    public static final String COLUMN = "table"; // Column name in MapR-DB that contains the device table mapping


}
