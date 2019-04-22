package day4;

import day3.Movie;
import day3.MovieBean;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.TreeSet;

public class Movie3 {
    public static class MapTask extends Mapper<LongWritable, Text,Text,MovieBean> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //json解析
            try {
                ObjectMapper mapper = new ObjectMapper();
                MovieBean bean = mapper.readValue(value.toString(), MovieBean.class);
                String movie = bean.getMovie();
                context.write(new Text(movie), bean);
            }catch (Exception e){
                System.err.println("跳过该错误");
            }
        }
    }
    public static class ReduceTask extends Reducer<Text,MovieBean,MovieBean,NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<MovieBean> values, Context context) throws IOException, InterruptedException {

            //当数据量特别大的时候，内存会溢出
            TreeSet<MovieBean> tree = new TreeSet<MovieBean>(new Comparator<MovieBean>() {
                @Override
                public int compare(MovieBean o1, MovieBean o2) {
                    return o2.getRate()-o1.getRate();
                }
            });
            //拿到数据添加到treeset
            for (MovieBean value : values) {
                if(tree.size()<5){
                    tree.add(value);
                }else{
                    MovieBean last = tree.last();
                    if(last.getRate()<value.getRate()){
                        tree.remove(last);
                        tree.add(value);
                    }
                }
            }
            for (MovieBean movieBean : tree) {
                context.write(movieBean,NullWritable.get());
            }
        }
    }
    public static void main(String[] args) throws Exception {
        org.apache.hadoop.conf.Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        //设置类型
        job.setMapperClass(Movie3.MapTask.class);
        job.setReducerClass(Movie3.ReduceTask.class);
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
        FileOutputFormat.setOutputPath(job,new Path("D:\\add\\a"));
        boolean b = job.waitForCompletion(true);
        System.out.println(b?"修改成功":"修改错误");

    }

}
