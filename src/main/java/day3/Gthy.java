package day3;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;

public class Gthy {

    public static class MakTask extends Mapper<LongWritable, Text,Text,Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //先切分 :
            String[] splits = value.toString().split(":");
            String user = splits[0];
            String lists = splits[1];
            //以 , 切分
            String[] friends = lists.split(",");
            for (String friend : friends) {
                context.write(new Text(friend),new Text(user));
            }
        }
    }
    public static class ReduceTask extends Reducer<Text,Text,Text,Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //用来放拼接 字符串的
            StringBuffer stringBuffer = new StringBuffer();
            boolean flag = true;
            for (Text value : values) {
                if (flag){
                    stringBuffer.append(value);
                    flag=false;
                }else {
                    stringBuffer.append(",").append(value);
                }
            }
            //写出去
            context.write(new Text(key),new Text(stringBuffer.toString()));
        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        //设置类型
        job.setMapperClass(Gthy.MakTask.class);
        job.setReducerClass(Gthy.ReduceTask.class);
        job.setJarByClass(Gthy.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //文件存在删除
        File file = new File("D:\\add");
        if(file.exists()){
            FileUtils.deleteDirectory(file);
        }
        FileInputFormat.addInputPath(job,new Path("D:\\111\\linxu命令\\friend.txt"));
        FileOutputFormat.setOutputPath(job,new Path("D:\\add"));
        boolean b = job.waitForCompletion(true);
        System.out.println(b?"修改成功":"修改错误");

    }
}
