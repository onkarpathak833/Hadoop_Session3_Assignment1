package Session03.Assignment01.Task03;

import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import Session03.Assignment01.Task02.Session03_Task02;

public class Session03_Task03 {

	
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		Session03_Task02 session = new Session03_Task02();
		Scanner sc = new Scanner(System.in);
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://localhost:9000");
		FileSystem fs = FileSystem.get(conf);
		String hdfsPath = sc.nextLine();//"/user/acadgild";
		String[] hdfsPathArray = hdfsPath.split(",");
		for(String currentPath:hdfsPathArray) {
		System.out.println("Current Procesing Path is : "+currentPath);
		Path path = new Path(currentPath);
		FileStatus[] status = fs.listStatus(path);
		
		for(FileStatus stat : status) {
			if(stat.isDirectory()) {
				String dirPath = stat.getPath().toString();
				System.out.println("Directory under : "+stat.getPath().getParent().toString()+" is : "+stat.getPath().toString());
				session.listDirectoryAndFiles(dirPath);
			}
			if(stat.isFile()) {
				System.out.println("File under directory : "+stat.getPath().getParent().toString()+" is : "+stat.getPath().toString());
			}
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
			System.out.println("File under : "+status.getPath().getParent().toString()+" is :: "+status.getPath().toString());
		}
		}
	}


}
