import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class PhraseLibraryBuilder {
    // Mapper类，将文本转换为phrase并输出键值对（phrase, 1）
    public static class PhraseLibraryMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private int phrase_length; // phrase的大小
        // 在任务启动前进行一次初始化，获取phrase的大小
        @Override
        public void setup(Context context) {
            Configuration conf = context.getConfiguration();
            phrase_length = conf.getInt("phrase_length", 50);
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
            // 生成并输出每个phrase的键值对
            StringBuilder sb;
            sb = new StringBuilder(phrase_length);
            for (int i = 0; i < words.length; ++i) {
                sb.append(words[i]);
                sb.append(" ");
                if (sb.length() >= phrase_length || i == words.length - 1){
//                    System.out.print(sb.toString().trim() + '=' + key.toString());
                    context.write(new Text(sb.toString().trim() + '=' + key.toString()), new IntWritable(1));
                    sb = new StringBuilder(phrase_length);
                }
            }
        }
    }

    // Reducer类，对Mapper输出的中间键值对进行汇总，输出最终的键值对（phrase, 总计数）
    public static class PhraseLibraryReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        // reduce方法，对每个phrase的计数进行求和
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
//            System.out.print("Reducer1: key: " + key.toString() + "value: " + values.toString());
            int sum = 0;
            // 对每个phrase的计数进行累加
            for (IntWritable value : values) {
                sum += value.get();
            }
            // 输出最终的键值对
//            System.out.print(key+": "+sum+"\n");
            context.write(key, new IntWritable(sum));
        }
    }
}
