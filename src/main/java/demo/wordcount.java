package demo;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class wordcount {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> textDs = env.readTextFile("E:\\mycode\\java\\flinkdemo\\src\\main\\resources\\data\\word1.txt");
        SingleOutputStreamOperator<Tuple2<String, Long>> wordOneTuple = textDs.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            String[] words = line.split(" ");
            for (String word : words) {
                out.collect(Tuple2.of(word, 1L));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.LONG));
        wordOneTuple.print();
        KeyedStream<Tuple2<String, Long>, String> keyedStream = wordOneTuple.keyBy(data -> data.f0);
        SingleOutputStreamOperator<Tuple2<String, Long>> sum = keyedStream.sum(1);
        sum.print();
        try {
            env.execute("wordcount");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
