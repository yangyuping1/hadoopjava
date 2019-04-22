package day3;

import day2.MapreduceDemo;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.FileUtil;
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

public class Phone {
    //mapper端
    public static class MapTask extends Mapper<LongWritable, Text,Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] splits = value.toString().split("\t");
            if (splits.length>=7){
                String phone = splits[0].substring(0,3);//取前三位
                String province = splits[1];
                String yys = splits[3];
                context.write(new Text(phone+"\t"+province),new Text(yys));//拼接..
            }
        }
    }
    //reduced端
    public static class ReducerTask extends Reducer<Text,Text,Text,Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(new Text(key), new Text(value));
                break;
            }
        }
    }

        public static void main(String[] args) throws Exception {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf);
            //设置类型
            job.setMapperClass(Phone.MapTask.class);
            job.setReducerClass(Phone.ReducerTask.class);
            job.setJarByClass(Phone.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            //文件存在删除
            File file = new File("D:\\add");
            if(file.exists()){
                FileUtils.deleteDirectory(file);
            }
            FileInputFormat.addInputPath(job,new Path("D:\\111\\linxu命令\\Phone.txt"));
            FileOutputFormat.setOutputPath(job,new Path("D:\\add"));

            boolean b = job.waitForCompletion(true);
            System.out.println(b?"修改成功":"修改错误");

        }
    }
