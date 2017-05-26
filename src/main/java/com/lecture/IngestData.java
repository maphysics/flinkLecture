package com.lecture;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

/**
 * Created by Melissa on 5/24/2017.
 */
public class IngestData {

    static DataSet<String> getNews(){
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //TODO! update location!
        //TODO! Use csvReader
        DataSet<String> news = env.readTextFile("file:///C:\\Users\\Melissa\\flinkLecture\\src\\main\\resources\\news.csv");

        return news;
    }

}

