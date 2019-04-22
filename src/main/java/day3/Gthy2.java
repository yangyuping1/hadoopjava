package day3;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

public class Gthy2 {
    public static class MakTask extends Mapper<LongWritable, Text,Text,Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] splits = value.toString().split("\t");
            String val = splits[0];
            String users = splits[1];
            String[] zuhe = users.split(",");
            Arrays.sort(zuhe);
            for (int i = 0; i < zuhe.length-1; i++) {
                for (int j = i+1; j < zuhe.length; j++) {
                    String line = zuhe[i]+"-"+zuhe[j];
                    context.write(new Text(line),new Text(val));
                }
            }
        }
    }
    public static class ReduceTask extends Reducer<Text,Text,Text,Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuffer sb = new StringBuffer();
            boolean flag = true;
            for (Text value : values) {
                if (flag){
                    sb.append(value);
                    flag=false;
                }else {
                    sb.append(",").append(value);
                }
            }
            //写出去
            context.write(new Text(key),new Text(sb.toString()));
        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        //设置类型
        job.setMapperClass(Gthy2.MakTask.class);
        job.setReducerClass(Gthy2.ReduceTask.class);
        job.setJarByClass(Gthy2.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //文件存在删除
        File file = new File("D:\\add\\a");
        if(file.exists()){
            FileUtils.deleteDirectory(file);
        }
        FileInputFormat.addInputPath(job,new Path("D:\\add"));
        FileOutputFormat.setOutputPath(job,new Path("D:\\add\\a"));
        boolean b = job.waitForCompletion(true);
        System.out.println(b?"修改成功":"修改错误");

    }
}
