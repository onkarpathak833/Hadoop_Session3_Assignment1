package Session03.Assignment01.Task02;

import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Session03_Task02 {
	
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		Session03_Task02 session = new Session03_Task02();
		Scanner sc = new Scanner(System.in);
		String hdfsPath = sc.nextLine(); //"/user/acadgild";
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://localhost:9000");
		FileSystem fs = FileSystem.get(conf);
		
		Path path = new Path(hdfsPath);
		FileStatus[] status = fs.listStatus(path);
		
		for(FileStatus stat : status) {
			if(stat.isDirectory()) {
				String dirPath = stat.getPath().toString();
				System.out.println("Directory under : "+stat.getPath().getParent().toString()+" is : "+stat.getPath().toString());
				session.listDirectoryAndFiles(dirPath);
			}
			if(stat.isFile()) {
				System.out.println("File under directory : "+stat.getPath().getParent().toString()+" is :: "+stat.getPath().toString());
			}
		}
	}
	public void listDirectoryAndFiles(String path) throws IOException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://localhost:9000");
		FileSystem fs = FileSystem.get(conf);	
		Path tempPath = new Path(path);
		FileStatus[] temp = fs.listStatus(tempPath);
		for(FileStatus status:temp) {
		if(status.isDirectory()) {
			System.out.println("Direcory under : "+status.getPath().getParent().toString()+" is  : "+status.getPath().toString());
			
			listDirectoryAndFiles(status.getPath().toString());
		}
		if(status.isFile()) {
			System.out.println("File under directory : "+status.getPath().getParent().toString()+" is :: "+status.getPath().toString());
		}
		}
	}

}
