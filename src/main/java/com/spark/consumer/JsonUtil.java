package com.spark.consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by ranjitlingaiah on 1/18/17.
 */
public class JsonUtil {

    private static void flattenJsonNode(JsonNode jsonNode, String key) {
        JsonNode enrichedNode = jsonNode.get(key);

        System.out.println(enrichedNode.toString());
        ObjectNode origJson = (ObjectNode) jsonNode;
        if (enrichedNode.isObject()) {
            ObjectNode objectNode = (ObjectNode) enrichedNode;
            Iterator<Map.Entry<String, JsonNode>> iter = objectNode.fields();

            while (iter.hasNext()) {
                Map.Entry<String, JsonNode> entry = iter.next();
                origJson.set(entry.getKey(), entry.getValue());
            }
        }

        origJson.remove(key);
    }

    private static String transformedDate(String dateString) {
        SimpleDateFormat origDateFormat, desiredOutputFormat;
        origDateFormat = new SimpleDateFormat(Constants.INCOMING_TIME_PATTERN);
        String outputDateString = null;
        try {
            Date date = origDateFormat.parse(dateString);
            desiredOutputFormat = new SimpleDateFormat(Constants.PARTITION_DATE_PATTERN);
            System.out.println("Timestamp in JSON record:" + dateString );
            outputDateString = desiredOutputFormat.format(date);
            System.out.println("New Date:"+ outputDateString);
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        return  outputDateString;
    }

    private static void mapDeviceType(JsonNode jsonNode, String tablePath) {
        JsonNode deviceTypeNode = jsonNode.get(Constants.DEVICE_TYPE_IN_JSON);
        String deviceType = deviceTypeNode.asText();
        System.out.println("Device Type:" + deviceType);
        String mappedTable = HBaseUtil.deviceTableMapping(tablePath, deviceType, Constants.CF, Constants.COLUMN, 1);
        System.out.println("Mapped Device Type:" + mappedTable);
        ((ObjectNode) jsonNode).put(Constants.TABLE_OUT_JSON, mappedTable);
        String transformedDate = transformedDate(jsonNode.get(Constants.TIME_IN_JSON).asText());
        ((ObjectNode) jsonNode).put(Constants.DATE_OUT_JSON, transformedDate);
    }

    public static JsonNode transformJson(JsonNode jsonNode, String tablePath) {

        flattenJsonNode(jsonNode, Constants.ENRICHED_FIELDS);
        flattenJsonNode(jsonNode, Constants.PARSED_FIELDS);

        mapDeviceType(jsonNode, tablePath);

        ObjectNode on = (ObjectNode) jsonNode;
        if (jsonNode.isObject()) {
            ObjectNode objectNode = (ObjectNode) jsonNode;
            Iterator<Map.Entry<String, JsonNode>> iter = objectNode.fields();

            while (iter.hasNext()) {
                Map.Entry<String, JsonNode> entry = iter.next();
                on.set(entry.getKey(), entry.getValue());
            }
        }

        return jsonNode;
    }

    private static String getDeviceType(JsonNode jsonNode, String key) {
        JsonNode deviceTypeNode = jsonNode.get(key);
        return deviceTypeNode.asText();
    }
}
