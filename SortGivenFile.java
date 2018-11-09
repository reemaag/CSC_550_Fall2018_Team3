

import java.io.FileWriter;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;


public class SortGivenFile extends MapValues{
	
	public static void main(String[] args) throws Exception {
		
		System.out.print("Data generation started");
		
   		FileWriter writer = new FileWriter("/home/osboxes/eclipse-workspace/Big-Data-Project/input/file1.csv", true) ;
   		DataGeneration.createfile(writer,"/home/osboxes/eclipse-workspace/Big-Data-Project/input/file1.csv");
   				   	
   		System.out.print("Data generation completed");
   		
   		System.out.println("Copying files to hadoop");
      	 DataGeneration.copyFilestoHadoop();
		
      	System.out.println("Start mapper and reducer");
      	
		Path fileInput = new Path("hdfs://localhost:9000/Big-Data-Project/hdfs/file1.csv");

		Path fileOutput = new Path("hdfs://localhost:9000/Big-Data-Project/hdfs/output");

		Job sortJob = Job.getInstance();	//Job for the Hadoop Cluster

		sortJob.setJarByClass(SortGivenFile.class);
		
		sortJob.setJobName("hadoopJobSortFile");	//Specifying the Job Parameters		

		sortJob.setMapperClass(MapValues.class);	//Set the Mapper task for the cluster
			
		sortJob.setReducerClass(ReduceMappedValues.class);	//Set the Reducer task for the cluster
		
		sortJob.setNumReduceTasks(1);	//Set the number of reducer tasks


		// The following set of codes will specify the key/value of the file into the mapper 

		sortJob.setMapOutputKeyClass(Text.class);

		sortJob.setMapOutputValueClass(IntWritable.class);

		sortJob.setOutputKeyClass(Text.class);

		sortJob.setOutputValueClass(IntWritable.class);
		
		
		// The following set of codes will specify the file input to be loaded 

		FileInputFormat.addInputPath(sortJob, fileInput);

		sortJob.setInputFormatClass(TextInputFormat.class);

		// The following set of codes will specify the file output to be generated

		FileOutputFormat.setOutputPath(sortJob, fileOutput);

		sortJob.setOutputFormatClass(TextOutputFormat.class);

		//The following set of codes will initialize the job

		int executeJob = sortJob.waitForCompletion(true) ? 0 : 1;
		
		System.out.println("Start File Validation");		
		
		int res = ToolRunner.run(new JobConf(), new ValidateGivenFile(), args);
	    System.exit(res);
	  

		}
	
	
		    

}
