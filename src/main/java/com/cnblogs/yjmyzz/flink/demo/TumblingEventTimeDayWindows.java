package com.cnblogs.yjmyzz.flink.demo;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;

@PublicEvolving
public class TumblingEventTimeDayWindows extends WindowAssigner<Object, TimeWindow> {
    private static final long serialVersionUID = 2796362621836725145L;

    private static final long TOTAL_MILISECONDS_OF_DAY = 24 * 3600 * 1000L;

    private final static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    public static long getDateBegin(long timestamp) {
        Calendar c = Calendar.getInstance();
        c.setTimeInMillis(timestamp);
        c.set(Calendar.HOUR_OF_DAY, 0);
        c.set(Calendar.MINUTE, 0);
        c.set(Calendar.SECOND, 0);
        c.set(Calendar.MILLISECOND, 0);
        return c.getTimeInMillis();
    }

    protected TumblingEventTimeDayWindows() {

    }

    @Override
    public Collection<TimeWindow> assignWindows(Object element, long timestamp, WindowAssignerContext context) {
        if (timestamp > Long.MIN_VALUE) {
            long start = TumblingEventTimeDayWindows.getDateBegin(timestamp);
            long end = start + TOTAL_MILISECONDS_OF_DAY - 1;
            System.out.println("start:" + start + ",end:" + end + " ; window range:" + sdf.format(new Date(start)) + " ~ " + sdf.format(new Date(end)));
            return Collections.singletonList(new TimeWindow(start, end));
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


    public static TumblingEventTimeDayWindows of() {
        return new TumblingEventTimeDayWindows();
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