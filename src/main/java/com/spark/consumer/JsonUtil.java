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
        origDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String outputDateString = null;
        try {
            Date date = origDateFormat.parse(dateString);
            desiredOutputFormat = new SimpleDateFormat("dd-MM-yyyy");
            System.out.println("Timestamp in JSON record:" + dateString );
            outputDateString = desiredOutputFormat.format(date);
            System.out.println("New Date:"+ outputDateString);
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        return  outputDateString;
    }

    private static void mapDeviceType(JsonNode jsonNode) {
        JsonNode deviceTypeNode = jsonNode.get("device_type");
        String deviceType = deviceTypeNode.asText();
        System.out.println("Device Type:" + deviceType);
        String mappedTable = HBaseUtil.deviceTableMapping(Constants.TABLE_MAPPING, deviceType, Constants.CF, Constants.COLUMN, 1);
        System.out.println("Mapped Device Type:" + mappedTable);
        ((ObjectNode) jsonNode).put("table", mappedTable);
        String transformedDate = transformedDate(jsonNode.get("time").asText());
        ((ObjectNode) jsonNode).put("date", transformedDate);
    }

    public static JsonNode transformJson(JsonNode jsonNode) {

        flattenJsonNode(jsonNode, Constants.ENRICHED_FIELDS);
        flattenJsonNode(jsonNode, Constants.PARSED_FIELDS);

        mapDeviceType(jsonNode);

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
