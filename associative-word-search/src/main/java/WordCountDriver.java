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


public class WordCountDriver {
    public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
        long start = System.currentTimeMillis();
        // 从命令行参数中获取文本输入路径、中间结果输出路径、N-Gram长度、阈值、TopK单词数
        String inputPath = "hdfs://0.0.0.0:9000/dataset/";
        String phrase_length = "100";

        // Job1配置
        Configuration conf1 = new Configuration();
        // 设置行分隔符为标点符号
        conf1.set("textinputformat.record.delimiter", "[.,;:!?'\"]");
        conf1.set("phrase_length", phrase_length);
        Job job1 = Job.getInstance(conf1);
        job1.setJobName("phraseCount");
        job1.setJarByClass(WordCountDriver.class);
        // 设置Mapper和Reducer类
        job1.setMapperClass(PhraseLibraryBuilder.PhraseLibraryMapper.class);
        job1.setReducerClass(PhraseLibraryBuilder.PhraseLibraryReducer.class);
        // 设置输出键值类型
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        // 设置输入输出格式
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(PhraseOutput.class);
        // 设置输入和输出路径
        TextInputFormat.setInputPaths(job1, new Path(inputPath));
        job1.waitForCompletion(true);
        long end = System.currentTimeMillis();
        long duration = (end - start);
        System.out.println("程序运行时间：" + duration + "ms");
    }

}
