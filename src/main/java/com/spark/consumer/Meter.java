package com.spark.consumer;

/**
 * Created by ranjitlingaiah on 2/2/17.
 */

import java.io.Serializable;

public class Meter implements Serializable{

    private String meterID;
    private String meterStatus;
    private String meterTime;

    public String getMeterID() {
        return meterID;
    }

    public void setMeterID(String meterID) {
        this.meterID = meterID;
    }

    public String getMeterStatus() {
        return meterStatus;
    }

    public void setMeterStatus(String meterStatus) {
        this.meterStatus = meterStatus;
    }

    public String getMeterTime() {
        return meterTime;
    }

    public void setMeterTime(String meterTime) {
        this.meterTime = meterTime;
    }
}
