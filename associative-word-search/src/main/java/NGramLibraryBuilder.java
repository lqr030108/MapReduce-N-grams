import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import java.io.IOException;


public class NGramLibraryBuilder {
    // Mapper类，将文本转换为N-grams并输出键值对（N-gram, 1）
    public static class NGramMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private int numGram; // N-gram的大小
        // 在任务启动前进行一次初始化，获取N-gram的大小
        @Override
        public void setup(Context context) {
            Configuration conf = context.getConfiguration();
            numGram = conf.getInt("numGram", 3);
        }
        // map方法，处理每一行文本并输出N-grams的键值对
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
//            System.out.print("Mapper1: key: " + key.toString() + "value: " + value.toString());
            String line = value.toString();
            // 规范化文本，转小写并去除非字母字符
            line = line.trim().toLowerCase();
            line = line.replaceAll("[^a-z]", " ");
            // 将文本分割成单词数组
            String[] words = line.trim().split("\\s+");
            // 如果单词数量小于2，发出警告并跳过
            if (words.length < 2) {
                String info = String.format(
                        "Warning: The length of words array is %d. Requires at least 2. Origin line: %s",
                        words.length, line);
                context.getCounter("Warnings", info);
                return;
            }
            // 生成并输出每个N-gram的键值对
            StringBuilder sb;
            for (int i = 0; i < words.length - 1; ++i) {
                sb = new StringBuilder(numGram);
                sb.append(words[i]);
                for (int j = 1; i + j < words.length && j < numGram; ++j) {
                    sb.append(" ");
                    sb.append(words[i + j]);
                    context.write(new Text(sb.toString().trim()), new IntWritable(1));
                }
            }
        }
    }

    // Reducer类，对Mapper输出的中间键值对进行汇总，输出最终的键值对（N-gram, 总计数）
    public static class NGramReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        // reduce方法，对每个N-gram的计数进行求和
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
//            System.out.print("Reducer1: key: " + key.toString() + "value: " + values.toString());
            int sum = 0;
            // 对每个N-gram的计数进行累加
            for (IntWritable value : values) {
                sum += value.get();
            }
            // 输出最终的键值对，键是N-gram，值是总计数
            context.write(key, new IntWritable(sum));
        }
    }
}
