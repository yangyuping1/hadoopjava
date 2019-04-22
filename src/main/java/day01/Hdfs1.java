package day01;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.URI;


public class Hdfs1 {
    FileSystem fileSystem = null;
    @Before
    public void init() throws Exception {
        Configuration configuration =new Configuration();
        fileSystem =FileSystem.get(new URI("hdfs://hadoop01:9000"),configuration,"root");
    }
    //上传一个文件
    @Test
    public void upload(){
        try {
            fileSystem.copyFromLocalFile(new Path("D:\\kgc.txt"),new Path("/xx"));

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    //下载
    @Test
    public void dowload(){
        try {
            fileSystem.copyToLocalFile(new Path("/xx/kgc.txt"),new Path("D:\\111"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    //创建文件夹
    @Test
        public void mkdir(){
        try {
            fileSystem.mkdirs(new Path("/baidu"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    //删除文件夹
    @Test
    public void remover(){
        try {
            fileSystem.delete(new Path("/baidu"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    //查看信息
    @Test
    public void cat() throws Exception {
        RemoteIterator<LocatedFileStatus> iterator = fileSystem.listFiles(new Path("/"),true);
        while (iterator.hasNext()){
            LocatedFileStatus fileStatus = iterator.next();
            System.out.println("文件路径是:"+fileStatus.getLen());
            System.out.println("副本数为:"+fileStatus.getBlockLocations());
            System.out.println("bloc为:"+fileStatus.getReplication());
        }
    }
    //读写数据
    @Test
    public void readWrite() throws Exception {
        FSDataInputStream open =fileSystem.open(new Path("/hadoop/kgc.txt"));
        FSDataOutputStream create = fileSystem.create(new Path("/wo/"));
        //读取数据
        BufferedReader br =new BufferedReader(new InputStreamReader(open));
        BufferedWriter bw= new BufferedWriter(new OutputStreamWriter(create));
        //循环读取数据
        String line = null;
        //行数
        while ((line =br.readLine())!= line){
            bw.write(line);
            bw.newLine();
            bw.flush();
        }
        bw.close();
        br.close();
    }
    @After
    public void close(){
        try {
            fileSystem.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
