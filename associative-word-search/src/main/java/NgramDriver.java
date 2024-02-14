import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.io.IOException;


public class NgramDriver {
    public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
        long start = System.currentTimeMillis();
        // 从命令行参数中获取文本输入路径、中间结果输出路径、N-Gram长度、阈值、TopK单词数
        String inputPath = "hdfs://0.0.0.0:9000/dataset/";
        String nGramLibPath = "hdfs://0.0.0.0:9000/interim_output";
        String numberOfNGram = "5";
        String threashold = "1";
        String topkFollowingWords = "1000";

        // Job1配置
        Configuration conf1 = new Configuration();
        // 设置行分隔符为标点符号
        conf1.set("textinputformat.record.delimiter", "[.,;:!?'\"]");
        conf1.set("numGram", numberOfNGram);
        Job job1 = Job.getInstance(conf1);
        job1.setJobName("NGram");
        job1.setJarByClass(NgramDriver.class);
        // 设置Mapper和Reducer类
        job1.setMapperClass(NGramLibraryBuilder.NGramMapper.class);
        job1.setReducerClass(NGramLibraryBuilder.NGramReducer.class);
        // 设置输出键值类型
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        // 设置输入输出格式
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);
        // 设置输入和输出路径
        TextInputFormat.setInputPaths(job1, new Path(inputPath));
        TextOutputFormat.setOutputPath(job1, new Path(nGramLibPath));
        job1.waitForCompletion(true);

        // Job2配置
        Configuration conf2 = new Configuration();
        // 设置阈值和TopK参数
        conf2.set("threashold", threashold);
        conf2.set("topK", topkFollowingWords);
        Job job2 = Job.getInstance(conf2);
        job2.setJobName("LanguageModel");
        job2.setJarByClass(NgramDriver.class);
        // 设置Map输出键值类型和Reducer输出键值类型
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        // 设置Mapper和Reducer类
        job2.setMapperClass(LanguageModel.LanguageModelMap.class);
        job2.setReducerClass(LanguageModel.LanguageModelReduce.class);
        // 设置输入输出格式
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(MySQLTextOutputFormat.class);
        // 设置输入路径
        TextInputFormat.setInputPaths(job2, new Path(nGramLibPath));
        job2.waitForCompletion(true);
        long end = System.currentTimeMillis();
        long duration = (end - start);
        System.out.println("程序运行时间：" + duration + "ms");
    }
}