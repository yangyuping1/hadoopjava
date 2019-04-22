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
import org.codehaus.jackson.map.ObjectMapper;


import java.io.File;
import java.io.IOException;

public class Movie {
    public static class MapTask extends Mapper<LongWritable, Text,Text, IntWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //把json数据转化为正常数据
            ObjectMapper mapper = new ObjectMapper();
            //利用mapper对象转化json
            MovieBean movieBean = null;
            movieBean=mapper.readValue(value.toString(),MovieBean.class);
            //求电影名称和电影分数
            String movie = movieBean.getMovie();
            Integer rate =movieBean.getRate();
            //写出去
            context.write(new Text(movie),new IntWritable(rate));
        }
    }

    public static class ReducerTask extends Reducer<Text,IntWritable,Text, DoubleWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            //定义两个变量去接收  总分数   总次数
            int sum =0;
            int count = 0 ;
            for (IntWritable rate : values) {
                count++;
                sum+=rate.get();
            }
            //跳出循环
            double v = 1.0f * sum / count;
            context.write(new Text(key),new DoubleWritable(v));
        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        //设置类型
        job.setMapperClass(Movie.MapTask.class);
        job.setReducerClass(Movie.ReducerTask.class);
        job.setJarByClass(Movie.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
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
