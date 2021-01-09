package com.cnblogs.yjmyzz.flink.demo;

import lombok.Data;

@Data
public class WordCountPojo {

    public String word;

    public long eventTimestamp;

    public String eventDateTime;
}
