package com.spark.consumer;

import com.mapr.org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;

import java.io.IOException;
import java.text.SimpleDateFormat;

/**
 * Created by ranjitlingaiah on 1/18/17.
 */

public class HBaseUtil {
    static Configuration config = HBaseConfiguration.create();
    static HTable dataTable = null;

    public static HTable initHTable(String tableName) {
        try {
            if (dataTable == null) {
                dataTable = new HTable(config, tableName);
            }
        } catch (IOException ioex) {
            ioex.printStackTrace();
        }
        return dataTable;
    }


    private static Result getResult(HTable table, String row, String cf,
                                    String col, int versions) throws IOException {
        Get g = new Get(Bytes.toBytes(row));
        g.setMaxVersions(versions);
        Result r = table.get(g);
        return r;
    }

    public static String deviceTableMapping(String hbaseTableName, String row, String cf,
                                            String col, int versions) {
        String mappedTableName = null;
        try {
            Result r = HBaseUtil.getResult(initHTable(hbaseTableName), row, cf, col, versions);
            Cell cell = r.getColumnLatestCell(Bytes.toBytes(cf),
                    Bytes.toBytes(col));
            mappedTableName = Bytes.toString(CellUtil.cloneValue(cell));
            System.out.println("Device Table mapped to:" + mappedTableName);
        } catch (IOException ioex) {
            ioex.printStackTrace();
        }

        return mappedTableName;
    }


    public static void main(String[] args) throws IOException {
        String tablePath = "/user/ranjitlingaiah/devicetablemapping";
        try {
            String tableName = HBaseUtil.deviceTableMapping(tablePath, "apacheag", Constants.CF, Constants.COLUMN, 1);
            System.out.println("Tablename:" + tableName);
        } catch (Exception e) {
            e.printStackTrace();
        }

        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

    }

}
