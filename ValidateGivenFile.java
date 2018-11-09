import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;

/*
* The following set codes will validate the data in the form of Tera Input Format
* and check whether the data is actually in the sorted order.
*/



public class ValidateGivenFile extends Configured implements Tool {
  private static final Text error = new Text("error");

  static class ValidateMapper extends MapReduceBase 
      implements Mapper<Text,Text,Text,Text> {
    private Text lastKey;
    private OutputCollector<Text,Text> output;
    private String filename;
    
    
    private String getFilename(FileSplit split) {
      return split.getPath().getName();
    }

    public void map(Text key, Text value, OutputCollector<Text,Text> output,
                    Reporter reporter) throws IOException {
      if (lastKey == null) {
        filename = getFilename((FileSplit) reporter.getInputSplit());
        output.collect(new Text(filename + ":begin"), key);
        lastKey = new Text();
        this.output = output;
      } else {
        if (key.compareTo(lastKey) < 0) {
          output.collect(error, new Text("misorder in " + filename + 
                                         " last: '" + lastKey + 
                                         "' current: '" + key + "'"));
        }
      }
      lastKey.set(key);
    }
    
    public void close() throws IOException {
      if (lastKey != null) {
        output.collect(new Text(filename + ":end"), lastKey);
      }
    }
  }

  
  static class ValidateReducer extends MapReduceBase 
      implements Reducer<Text,Text,Text,Text> {
    private boolean firstKey = true;
    private Text lastKey = new Text();
    private Text lastValue = new Text();
    public void reduce(Text key, Iterator<Text> values,
                       OutputCollector<Text, Text> output, 
                       Reporter reporter) throws IOException {
      if (error.equals(key)) {
        while(values.hasNext()) {
          output.collect(key, values.next());
        }
      } else {
        Text value = values.next();
        if (firstKey) {
          firstKey = false;
        } else {
          if (value.compareTo(lastValue) < 0) {
            output.collect(error, 
                           new Text("misordered keys last: " + 
                                    lastKey + " '" + lastValue +
                                    "' current: " + key + " '" + value + "'"));
          }
        }
        lastKey.set(key);
        lastValue.set(value);
      }
    }
    
  }

  public int run(String[] args) throws Exception {
    JobConf job = (JobConf) getConf();
    
    
    Path fileInput = new Path("hdfs://localhost:9000/Big-Data-Project/hdfs/output/part-r-00000");

	Path fileOutput = new Path("hdfs://localhost:9000/Big-Data-Project/hdfs/output1");
	TeraInputFormat.setInputPaths(job, fileInput);
    FileOutputFormat.setOutputPath(job, fileOutput);
    job.setJobName("ValidateGivenFile");
    job.setJarByClass(ValidateGivenFile.class);
    job.setMapperClass(ValidateMapper.class);
    job.setReducerClass(ValidateReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setNumReduceTasks(1);
    job.setLong("mapred.min.split.size", Long.MAX_VALUE);
    job.setInputFormat(TeraInputFormat.class);
    JobClient.runJob(job);
    return 0;
  }



}