package com.cnblogs.yjmyzz.flink.demo;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;

public class DayWindowAssigner extends WindowAssigner<Object, TimeWindow> {

    public static final long TOTAL_MILISECONDS_OF_DAY = 24 * 3600 * 1000L;

    private long getDateBegin(long timestamp) {
        Calendar c = Calendar.getInstance();
        c.setTimeInMillis(timestamp);
        c.set(Calendar.HOUR_OF_DAY, 0);
        c.set(Calendar.MINUTE, 0);
        c.set(Calendar.SECOND, 0);
        c.set(Calendar.MILLISECOND, 0);
        return c.getTimeInMillis();
    }

    public DayWindowAssigner() {

    }

    @Override
    public Collection<TimeWindow> assignWindows(Object element, long timestamp, WindowAssignerContext context) {
        if (timestamp > Long.MIN_VALUE) {
            long start = getDateBegin(timestamp);
            return Collections.singletonList(new TimeWindow(start, start + TOTAL_MILISECONDS_OF_DAY));
        } else {
            throw new RuntimeException("Record has Long.MIN_VALUE timestamp (= no timestamp marker). " +
                    "Is the time characteristic set to 'ProcessingTime', or did you forget to call " +
                    "'DataStream.assignTimestampsAndWatermarks(...)'?");
        }
    }

    @Override
    public Trigger<Object, TimeWindow> getDefaultTrigger(StreamExecutionEnvironment env) {
        return EventTimeTrigger.create();
    }

    @Override
    public TypeSerializer<TimeWindow> getWindowSerializer(ExecutionConfig executionConfig) {
        return new TimeWindow.Serializer();
    }

    @Override
    public boolean isEventTime() {
        return true;
    }
}
