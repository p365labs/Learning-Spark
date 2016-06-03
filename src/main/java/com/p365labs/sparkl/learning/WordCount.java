package com.p365labs.sparkl.learning;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;

/**
 * Created by federicopanini on 31/05/16.
 *
 * In this script a simple map/reduce is implemented to do a wordcount on a text file
 */
public class WordCount implements Serializable {

    private JavaSparkContext sc;

    /**
     * create JavaSparkContext and try to read a file and do a WordCount
     *
     * @param textfile
     * @param outputfile
     */
    public void doWordCound(String textfile, String outputfile) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("testspark");
        JavaSparkContext sc = new JavaSparkContext(conf);


        JavaRDD<String> file = sc.textFile(textfile);

        JavaRDD<String> words = file.flatMap(
                new FlatMapFunction<String, String>() {
                    public Iterable<String> call(String s) throws Exception {
                        return Arrays.asList(s.split("\\W+"));
                    }
                }

        );

        JavaPairRDD<String, Integer> counts = words.mapToPair(
                new PairFunction<String, String, Integer>() {
                    public Tuple2<String, Integer> call(String s) throws Exception {
                        return new Tuple2<String, Integer>(s.toLowerCase(), 1);
                    }
                }
        ).reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        JavaPairRDD<Integer, String> wordCounts = counts.mapToPair(
            new PairFunction<Tuple2<String, Integer>, Integer, String>() {
                public Tuple2<Integer, String> call(Tuple2<String, Integer> item) throws Exception {
                    return item.swap();
            }
        });

        JavaPairRDD countOrdered = wordCounts.sortByKey(false);

        JavaPairRDD<Integer, String> wordCountsReverse = countOrdered.mapToPair(
                new PairFunction<Tuple2<String, Integer>, Integer, String>() {
                    public Tuple2<Integer, String> call(Tuple2<String, Integer> item) throws Exception {
                        return item.swap();
                    }
                });

        wordCountsReverse.saveAsTextFile(outputfile);
    }
}
