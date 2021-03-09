package com.cnblogs.yjmyzz.flink.demo;

import com.google.gson.annotations.SerializedName;
import lombok.Data;

@Data
public class WordCountPojo {

    public String word;

    @SerializedName(value = "event_timestamp")
    public long eventTimestamp;

    @SerializedName(value = "event_datetime")
    public String eventDateTime;
}
