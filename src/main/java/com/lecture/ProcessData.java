package com.lecture;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.summarize.aggregation.IntegerSummaryAggregator;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.util.Collector;

/**
 * Created by Melissa on 5/24/2017.
 */
public class ProcessData {

    IngestData id = new IngestData();
    DataSet<Tuple2<Integer, Tuple5<String,String, String, Integer, Integer>>> tokenizedNews = id.getTokenizedNews();

    DstaSet<Tuple2<String, Integer>>

}
