package com.lecture;

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;

/**
 * Created by Melissa on 5/24/2017.
 */
public class CommentCountsByWord {

    public static void main(String[] args) throws Exception{
        DataSet<Tuple2<Integer, Tuple5<String, String, String, Integer, Integer>>> tuples = ProcessData.process(IngestData.getNews());

        DataSet<Tuple2<String, Integer>> tokenizedNews = tuples.flatMap(new Tokenizer());
        DataSet<Tuple2<String, Integer>> wordCommentCounts = tokenizedNews.groupBy(0).aggregate(Aggregations.SUM, 1);
        DataSet<Tuple2<String, Integer>> result = wordCommentCounts.sortPartition(1, Order.ASCENDING).setParallelism(1);
        result.print();
    }
}
