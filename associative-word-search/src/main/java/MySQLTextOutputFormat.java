import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * 将数据写入到mysql
 */
public class MySQLTextOutputFormat extends OutputFormat<Text,IntWritable> {
    protected  static class MySQLRecordWriter extends RecordWriter<Text,IntWritable> {
        private Connection connection = null;
        public MySQLRecordWriter(){
            //获取资源
            connection = JDBCUtil.getConnection();
        }
        /**
         * 输出数据,通过jdbc写入到mysql中
         * @param key    :reduce方法写出的key
         * @param value  :reduce方法写出的value值
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void write(Text key, IntWritable value) throws IOException, InterruptedException {
            PreparedStatement pstat = null;
            System.out.println("写入数据库！");
            try {
                String insertSQL = "insert into start_follow_words(start_phrase,follow_words,count)" +
                        " values(?,?,?)";
                pstat = connection.prepareStatement(insertSQL);
                //取得reduce方法传过来的key
                String[] curVal = key.toString().split("=");
                String start_phrase = curVal[0].trim();
                String follow_words = curVal[1].trim();
                String count = value.toString();
                pstat.setString(1,start_phrase);
                pstat.setString(2,follow_words);
                pstat.setInt(3,Integer.parseInt(count));
                //执行向数据库插入操作
                pstat.executeUpdate();
            }catch (SQLException e){
                e.printStackTrace();
            }finally {
                if(pstat != null){
                    try {
                        pstat.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        /**
         * 释放资源
         * @param taskAttemptContext
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            if(connection != null){
                try {
                    connection.close();
                }catch (SQLException e){
                    e.printStackTrace();
                }
            }
        }
    }
    //@Test
    public void test(){
        MySQLRecordWriter tt = new MySQLRecordWriter();
        Connection con = JDBCUtil.getConnection();
        System.out.println("+++++++++++++" + con);
    }
    @Override
    public RecordWriter<Text, IntWritable> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new MySQLRecordWriter();
    }
    @Override
    public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException {
    }
    //下面这段代码，摘抄自源码
    private FileOutputCommitter committer = null;
    public static Path getOutputPath(JobContext job) {
        String name = job.getConfiguration().get("mapred.output.dir");
        return name == null?null:new Path(name);
    }
    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
        if(committer == null){
            Path output = getOutputPath(context);
            committer = new FileOutputCommitter(output, context);
        }
        return committer;
    }
}

