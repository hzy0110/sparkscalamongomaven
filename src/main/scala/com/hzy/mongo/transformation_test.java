package com.hzy.mongo;
/*


import groovy.lang.Tuple;
import java.util.Arrays;
import java.util.List;
//import org.apache.hadoop.hive.ql.parse.HiveParser_IdentifiersParser.sysFuncNames_return;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
//java中的元组使用scala中的Tuple
import scala.Tuple2;
*/
/**
 * @author
 * Spark算子演示：
 * Transformation类算子
 * map
 * flatMap
 * filter
 * sortByKey
 * reduceByKey
 * sample
 * Action类算子：
 * count
 * collect
 * foreach
 *//*

public class transformation_test {
    @SuppressWarnings("resource")
    public static void main(String[] args) {
//因为java是面向对象的语言，当使用java来写Spark代码的时候，是传递对象，自动的提示生成返回值可以简化开发
//快捷键：Ctrl+1
//Spark应用程序的配置文件对象，可以设置：1：运行模式，2：应用程序Application的名称，3：运行时的资源的需求
        SparkConf sparkConf = new SparkConf().setAppName("transformation_test").setMaster("local[3]");
//SparkContext是非常的重要的，它是通往集群的唯一的通道
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
//加载文件生成RDD
        JavaRDD<String> textFileRDD= sparkContext.textFile("words.txt");
//==========================filter(function(T,Boolean))=========================//
//filter算子是Transformation类算子，返回一个由通过filter()的函数的元素组成的RDD，结果为true的元素会返回，可以用于过滤
//第一个泛型是textFileRDD里内容的类型，Boolean是返回值类型
        JavaRDD<String> filterRDD = textFileRDD.filter(new Function<String, Boolean>() {
            */
/**
             * 分布式的程序：对象需要走网络传输
             * 添加序列化id
             *//*

            private static final long serialVersionUID = 1L;
            public Boolean call(String line) throws Exception {
//过滤掉java
                System.out.println("是否执行filter算子");
                return !line.contains("java");
            }
        });
//============================foreach========================================//
//foreach算子是Action类算子，遍历RDD的计算结果
        filterRDD.foreach(new VoidFunction<String>() {
            private static final long serialVersionUID = 1L;
            public void call(String word) throws Exception {
                System.out.println(word);
            }
        });
//================================collect=========================//
//collect算子是Action类算子：将在集群中运行任务的结果拉回Driver端
//注意：当计算结果很大的时候，会导致Driver端OOM
        List<String> list = filterRDD.collect();
        for(String string:list){
            System.out.println(string);
        }
//===============================================map=========================//
//map算子是transformation类算子，一般用于改变RDD的内数据的内容格式
//String输入数据的类型，第二个为返回值类型
        JavaRDD<Integer> mapRDD = textFileRDD.map(new Function<String, Integer>() {
            private static final long serialVersionUID = 1L;
            public Integer call(String line) throws Exception {
                return line.contains("java")?1:0;
            }
        });
        mapRDD.foreach(new VoidFunction<Integer>() {
            private static final long serialVersionUID = 1L;
            public void call(Integer num) throws Exception {
                System.out.println(num);
            }
        });
//============================================sample=========================//
//sample算子是一个Transformation类算子，通常用于大数据中的抽样
//withReplacement：是否为放回式的抽样，false为不放会式抽样。fraction为抽样的比例，seed为随机种子：随机抽样算法的初始值
        JavaRDD<String> sampleRDD = textFileRDD.sample(true, 0.5);
        long count= sampleRDD.count();
        System.out.println(count);
//=========================================flatmap=========================//
//flatmap:map+flat,input 1 output *
//map :input 1 output 1
//切分单词
        JavaRDD<String> flatMapRDD = textFileRDD.flatMap(new FlatMapFunction<String, String>() {
            private static final long serialVersionUID = 1L;
            //返回迭代器
            public Iterable<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" "));
            }
        });
        List<String> collect = flatMapRDD.collect();
        for(String string:collect){
            System.out.println("word="+string);
        }
//===============================sortByKey=========================//
//在java的API中：将RDD转化为(K,V)格式的RDD，需要使用**toPair
//第一个为输入数据的类型，第二个，第三个参数为返回的K，V
        List<Tuple2<String,Integer>> temp = flatMapRDD.mapToPair(new PairFunction<String, String, Integer>() {
            private static final long serialVersionUID = 1L;
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String, Integer>(word, 1);
            }
// reduceByKey为Transformation类算子
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            private static final long serialVersionUID = 1L;
            public Integer call(Integer v1, Integer v2) throws Exception {
//循环反复将v1+v2的值累加
                return v1+v2;
            }
//变换（K，V）格式的RDD
        }).mapToPair(new PairFunction<Tuple2<String,Integer>, Integer, String>() {
            private static final long serialVersionUID = 1L;
            public Tuple2<Integer, String> call(Tuple2<String, Integer> tuple)
                    throws Exception {
                return new Tuple2<Integer, String>(tuple._2, tuple._1);
            }
        }).sortByKey(false).mapToPair(new PairFunction<Tuple2<Integer,String>, String, Integer>() {
            private static final long serialVersionUID = 1L;
            public Tuple2<String, Integer> call(Tuple2<Integer, String> line)
                    throws Exception {
                return new Tuple2<String, Integer>(line._2,line._1);
            }
        }). collect();;
//注意：当使用本地的local[*>1]的时候，使用foreach遍历数据的时候会出错？
//具体的什么问题我也不是很清楚？
*/
/**
 foreach(new VoidFunction<Tuple2<String,Integer>>() {
 private static final long serialVersionUID = 1L;
 public void call(Tuple2<String, Integer> tuple) throws Exception {
 System.out.println(tuple);
 }
 });
 **//*

        for(Tuple2<String, Integer> list1:temp){
            System.out.println(list1);
        }
//关闭SparkContext
        sparkContext.stop();
    }
}
*/
