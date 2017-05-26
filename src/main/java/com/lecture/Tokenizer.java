package com.lecture;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.util.Collector;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Created by Melissa on 5/25/2017.
 */
public class Tokenizer implements FlatMapFunction<Tuple2<Integer, Tuple5<String, String, String, Integer, Integer>>, Tuple2<String, Integer>> {

    public void flatMap(Tuple2<Integer, Tuple5<String, String, String, Integer, Integer>> integerTuple5Tuple2, Collector<Tuple2<String, Integer>> collector) throws Exception {
        Tuple2<String, Integer> headlineComments = getInfo((Tuple5<String, String, String, Integer, Integer>) integerTuple5Tuple2.getField(1));
        String headline = headlineComments.getField(0);
        headline = headline.replaceAll("[^a-zA-Z ]", "");
        String[] words = (headline).split(" ");
        Set<String> stopWords = getStopWords();
        for(String word : words){
            if(!stopWords.contains(word)) {
                collector.collect(Tuple2.of(word, (Integer) headlineComments.getField(1)));
            }
        }
    }

    private Tuple2<String, Integer> getInfo(Tuple5<String, String, String, Integer, Integer> inputTuple){
        return Tuple2.of((String) inputTuple.getField(0), (Integer) inputTuple.getField(4));
    }

    private Set<String> getStopWords() throws Exception{
        Set<String> stopWords = new LinkedHashSet<String>();
        //TODO! Load via classpath
        BufferedReader SW= new BufferedReader(new FileReader("C:\\Users\\Melissa\\flinkLecture\\src\\main\\resources\\stopwords.txt"));
        for(String line;(line = SW.readLine()) != null;)
            stopWords.add(line.trim());
        SW.close();
        return stopWords;
    }
}
