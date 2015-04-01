package org.scott.Tasks;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Map;

/**
 * Created by slombard on 3/29/15.
 */
public class MessageTask implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(MessageTask.class);


    public void messageRun(JavaDStream<Map> inStream) {
        inStream.print();
    }
    public void sparkrun(JavaDStream<Integer> inStream) {
/*
        JavaPairDStream<Integer, Integer> mappedStream = inStream.mapToPair(
                new PairFunction<Integer, Integer, Integer>() {

                    public Tuple2<Integer, Integer> call(Integer x) {
                        //LOG.info("X = {}", x);
                        return new Tuple2<>(x % 10, 1);
                    }
                }
        );
        JavaPairDStream<Integer, Integer> reducedStream = mappedStream.reduceByKey(
                new Function2<Integer, Integer, Integer>() {

                    public Integer call(Integer i1, Integer i2) {
                        //LOG.info("i1 = {}, i2 = {}", i1, i2);
                        return i1 + i2;
                    }
                });
        reducedStream.foreachRDD(new Function<JavaPairRDD<Integer, Integer>, Void>() {
            public Void call(JavaPairRDD<Integer, Integer> rdd) {
                for (Tuple2<Integer, Integer> i : rdd.take(10)) {
                    //LOG.info("RDD Data: {}", i);
                }
                return null;
            }
        });*/
    }

}

