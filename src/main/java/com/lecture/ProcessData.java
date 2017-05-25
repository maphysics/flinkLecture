package com.lecture;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.*;

/**
 * Created by Melissa on 5/24/2017.
 */
public class ProcessData {

    public static void process(DataSet<String> news) {
        DataSet<String> newsFiltered = news.filter(new FilterFunction<String>() {
            public boolean filter(String s) throws Exception {
                String[] pieces = s.split(",");
                if (pieces[0].equals("")) {
                    return false;
                } else {
                    return true;
                }
            }
        });

        DataSet<String> newsFilteredAgain = newsFiltered.filter(new FilterFunction<String>() {
            //TODO! Deal with Headlines containing commas
            //TODO! Tee rejected rows to logfile for review
            public boolean filter(String s) throws Exception {
                if (s.split(",").length == 6) {
                    return true;
                } else {
                    return false;
                }
            }
        });

        DataSet<Tuple2<Integer, Tuple5<String, String, String, Integer, Integer>>> tokenizedNews = newsFilteredAgain.map(new MapFunction<String, Tuple2<Integer, Tuple5<String, String, String, Integer, Integer>>>() {
            public Tuple2<Integer, Tuple5<String, String, String, Integer, Integer>> map(String s) throws Exception {
                String[] columns = s.split(",");

                Integer col0 = Integer.parseInt(columns[0]);
                Integer col4 = Integer.parseInt(columns[4]);
                Integer col5 = Integer.parseInt(columns[5]);
                Tuple5<String, String, String, Integer, Integer> innerTuple = Tuple5.of(columns[1], columns[2], columns[3], col4, col5);
                Tuple2<Integer, Tuple5<String, String, String, Integer, Integer>> row = Tuple2.of(col0, innerTuple);
                return row;
            }
        });

        //TODO! Add flatmap
//        DstaSet<Tuple2<String, Integer>>

    }
}
