package demo;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class SorceCustomTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(100L);
        env.setStateBackend(new EmbeddedRocksDBStateBackend());
        //// 每隔 1 秒启动一次检查点保存
        env.enableCheckpointing(1000);
        // 配置存储检查点到文件系统
        // 配置存储检查点到 JobManager 堆内存
        env.getCheckpointConfig().setCheckpointStorage(new
                JobManagerCheckpointStorage());
// 配置存储检查点到文件系统
        env.getCheckpointConfig().setCheckpointStorage(new
                FileSystemCheckpointStorage("src/main/resources/checkpoint"));
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        // c.固定延迟重启策略（开发中使用），如下：如果有异常,每隔5s重启1次,最多3次
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                3, // 最多重启3次数
                5000// 重启时间间隔
        ));

        DataStream<Event> ds = env.addSource(new ClickSource()).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.timestamp;
                            }
                        }));
//        ds.print();
        KeyedStream<Event, String> keyedStream = ds.keyBy(new KeySelector<Event, String>() {
            @Override
            public String getKey(Event event) throws Exception {
                return event.user;
            }
        });
//        keyedStream.max("timestamp").print("max: ");;

//        SingleOutputStreamOperator<Tuple2<String, Long>> clicksByUser = keyedStream.map(new MapFunction<Event, Tuple2<String, Long>>() {
//                    @Override
//                    public Tuple2<String, Long> map(Event event) throws Exception {
//                        return Tuple2.of(event.user, 1L);
//                    }
//                }).keyBy(data -> data.f0)
//                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
//                    @Override
//                    public Tuple2<String, Long> reduce(Tuple2<String, Long> stringLongTuple2, Tuple2<String, Long> t1) throws Exception {
//                        return Tuple2.of(stringLongTuple2.f0, stringLongTuple2.f1 + t1.f1);
//                    }
//                });
//        clicksByUser.print("clicksByUser:");
//        SingleOutputStreamOperator<Tuple2<String, Long>> result = clicksByUser.keyBy(data -> "key")
//                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
//                    @Override
//                    public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
//                        return value1.f1 > value2.f1 ? value1 : value2;
//                    }
//                });
//        result.print("result:");

//        ds.map(new MyRichMapper()).print();

//        ds.keyBy(data -> data.user)
////                .countWindow(10,2) // 滑动技术窗口
////                .window(EventTimeSessionWindows.withGap(Time.seconds(2))) // 事件时间会话窗口
////                .window(SlidingEventTimeWindows.of(Time.hours(1),Time.minutes(5))) // 滑动事件事件窗口
//                .window(TumblingEventTimeWindows.of(Time.hours(1)))  ;// 滚动事件事件窗口
// 定义一个输出标签
        //  会涉及到泛型擦除
        OutputTag<Tuple2<String,Long>> late = new OutputTag<Tuple2<String,Long>>("late") {
        };

        SingleOutputStreamOperator<Tuple2<String, Long>> result = ds.map(new MapFunction<Event, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(Event event) throws Exception {
                        return Tuple2.of(event.user, 1L);
                    }
                }).keyBy(p -> p.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .allowedLateness(Time.milliseconds(10))
                .sideOutputLateData(late)
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> stringLongTuple2, Tuple2<String, Long> t1) throws Exception {
                        return Tuple2.of(stringLongTuple2.f0, stringLongTuple2.f1 + t1.f1);
                    }
                });
        result.print("result:");
        result.getSideOutput(late).print("late:");
        env.execute("SorceCustomTest");
    }

    // 实现一个自定义的富函数类
    public static class MyRichMapper extends RichMapFunction<Event, Integer> {


        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            System.out.println("open生命周期被调用 " + getRuntimeContext().getIndexOfThisSubtask() + "号任务启动");
        }

        @Override
        public Integer map(Event event) throws Exception {
            return event.url.length();
        }

        @Override
        public void close() throws Exception {
            super.close();
            System.out.println("close生命周期被调用 " + getRuntimeContext().getIndexOfThisSubtask() + "号任务结束");
        }
    }
}
