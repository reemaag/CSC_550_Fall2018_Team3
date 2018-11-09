

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class ReduceMappedValues extends Reducer<Text,IntWritable,Text,IntWritable> {
	
	   private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,Context context) throws java.io.IOException, InterruptedException
		{
			
			
			String[] namearray = key.toString().split("\r\n");
					
			List<String> Input = new ArrayList<String>();
			
			for(int i=0;i<namearray.length;i++)
			{
				Input.add(namearray[i]);
			}
			
			Collections.sort(Input);

			for ( String val : Input)
			{
				context.write(new Text(val),result );
			}
	
			
		}
}


