import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import java.io.IOException;
import java.util.*;


public class LanguageModel {
    // 用于处理键值对类型为 LongWritable、Text 的数据，输出键值对类型为 Text、Text
    public static class LanguageModelMap extends Mapper<LongWritable, Text, Text, Text> {
        // 阈值，用于筛选符合条件的数据
        private int threashold;
        // 在 Mapper 运行前的初始化方法，从配置中获取阈值参数。context: 上下文对象，用于获取配置信息
        @Override
        public void setup(Context context) {
            Configuration conf = context.getConfiguration();
            threashold = conf.getInt("threashold", 20);
        }
        /**
         * Map 方法，处理每一行输入数据。
         * @param key 输入键，表示行号
         * @param value 输入值，表示文本内容
         * @param context 上下文对象，用于写出数据或记录警告
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
//            System.out.print("Mapper2: key: " + key.toString() + "value: " + value.toString());
            // 检查输入值是否为空或只包含空白字符
            if (value == null || value.toString().trim().length() == 0) {
                String info = String.format(
                        "Warning: Value is null or value contains nothing. Origin line: %s",
                        value.toString());
                context.getCounter("Warnings", info);
                return;
            }
            // 获取并清理输入行
            String line = value.toString().trim();
            // 使用制表符分割单词和计数
            String[] wordsAndCount = line.split("\t");
            // 检查单词和计数数组的长度是否小于 2
            if (wordsAndCount.length < 2) {
                String info = String.format(
                        "Warning: The length of wordsAndCount is %d. Requires at least 2. Origin line: %s",
                        wordsAndCount.length, line);
                context.getCounter("Warnings", info);
                return;
            }
            // 提取短语和计数
            String phrase = wordsAndCount[0].trim();
            int count = Integer.parseInt(wordsAndCount[1]);
            // 根据阈值筛选数据
            if (count < threashold) {
                return;
            }
            // 获取短语中最后一个空格的索引
            int lastSpaceIndex = phrase.lastIndexOf(" ");
            // 提取起始短语和后续单词
            String starting_phrase = phrase.substring(0, lastSpaceIndex);
            String following_word = phrase.substring(lastSpaceIndex + 1);
            // 检查起始短语是否为空
            if (starting_phrase == null || starting_phrase.length() == 0) {
                String info = String.format(
                        "Warning: Starting phrase is null or its length equals to 0. Origin line: %s",
                        line);
                context.getCounter("Warnings", info);
                return;
            }
            // 输出起始短语和后续单词及计数
            context.write(new Text(starting_phrase), new Text(following_word + '=' + count));
        }
    }
    // 用于处理键值对类型为 Text、Text 的数据，输出键值对类型为 DBOutputWritable、NullWritable
    public static class LanguageModelReduce extends Reducer<Text, Text, Text, IntWritable> {
        // 前K个热门记录
        private int topK;
        // 在 Reducer 运行前的初始化方法，从配置中获取前K个热门记录的参数。context: 上下文对象，用于获取配置信息
        @Override
        public void setup(Context context) {
            Configuration conf = context.getConfiguration();
            topK = conf.getInt("topK", 5);
        }
        /**
         * Reduce 方法，处理相同 key 的一组 values。
         * @param key 输入键，表示起始短语
         * @param values 输入值的迭代器，表示后续单词及其计数
         * @param context 上下文对象，用于写出数据
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
//            System.out.print("Reducer2: key: " + key.toString() + "value: " + values.toString());
            // 用于存储相同计数的单词列表的映射
            Map<Integer, List<String>> map = new HashMap<Integer, List<String>>();
            // 遍历 values，将相同计数的单词归类到同一个计数下
            for (Text value : values) {
                String[] curVal = value.toString().split("=");
                String word = curVal[0].trim();
                Integer count = Integer.valueOf(curVal[1].trim());
                if (map.containsKey(count)) {
                    map.get(count).add(word);
                } else {
                    List<String> list = new ArrayList<String>();
                    list.add(word);
                    map.put(count, list);
                }
            }
            // 使用最小堆来获取前K个热门记录
            PriorityQueue<Node> heap = new PriorityQueue<Node>(topK + 1, new Comparator<Node>() {
                @Override
                public int compare(Node e1, Node e2) {
                    return e1.count - e2.count;
                }
            });
            // 遍历计数映射，将每个计数与对应的单词列表加入最小堆
            for (int count : map.keySet()) {
                heap.offer(new Node(count, map.get(count)));
                if (heap.size() > topK) {
                    heap.poll();
                }
            }
            // 从最小堆中取出前K个热门记录，写出到上下文中
            while (!heap.isEmpty()) {
                Node top = heap.poll();
                int keyCount = top.count;
                List<String> wordList = top.wordList;
                for (String word : wordList) {
                    context.write(new Text(key.toString() + '=' + word), new IntWritable(keyCount));
                }
            }
        }
        // 内部类 Node，表示具有相同计数的单词列表。
        class Node {
            public int count;
            public List<String> wordList;
            /**
             * 构造方法，初始化计数和单词列表。
             * @param count 计数
             * @param wordList 单词列表
             */
            public Node(int count, List<String> wordList) {
                this.count = count;
                this.wordList = wordList;
            }
        }
    }
}
