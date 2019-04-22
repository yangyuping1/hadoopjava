package day4;

import day3.Movie;
import day3.MovieBean;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.util.*;

//treeSet  解决内存溢出
public class Movie2 {
    public static class MayTask extends Mapper<LongWritable, Text,Text, MovieBean>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            ObjectMapper mapper = new ObjectMapper();
            MovieBean movieBean = mapper.readValue(value.toString(), MovieBean.class);
            String movie = movieBean.getMovie();
            context.write(new Text(movie),movieBean);
        }
    }
    public static class ReducerTask extends Reducer<Text,MovieBean,MovieBean, NullWritable>{
        @Override
        protected void reduce(Text key, Iterable<MovieBean> values, Context context) throws IOException, InterruptedException {
            List<MovieBean> lists = new ArrayList<>();
            for (MovieBean value : values) {
                lists.add(value);
            }
            //排序降序
            lists.sort(new Comparator<MovieBean>() {
                @Override
                public int compare(MovieBean o1, MovieBean o2) {
                    return o2.getRate()-o1.getRate();
                }
            });
            //求取前五
            for (int i = 0; i < 5; i++) {
                context.write(lists.get(i),NullWritable.get());
            }

        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        //设置类型
        job.setMapperClass(Movie2.MayTask.class);
        job.setReducerClass(Movie2.ReducerTask.class);
        job.setJarByClass(Movie2.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MovieBean.class);
        job.setOutputKeyClass(MovieBean.class);
        job.setOutputValueClass(NullWritable.class);
        //文件存在删除
        File file = new File("D:\\add");
        if(file.exists()){
            FileUtils.deleteDirectory(file);
        }
        FileInputFormat.addInputPath(job,new Path("D:\\111\\linxu命令\\movie.json"));
        FileOutputFormat.setOutputPath(job,new Path("D:\\add"));
        boolean b = job.waitForCompletion(true);
        System.out.println(b?"修改成功":"修改错误");

    }
}
