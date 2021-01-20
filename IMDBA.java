import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.IntWritable;
import java.util.Iterator;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;


public class ReduceJoin
{
	public static class TitleMapper extends Mapper<LongWritable, Text, Text, Text> 
    {
        public void map(LongWritable k, Text value, Context context) throws IOException, InterruptedException 
        {
                          
            String[] lines = value.toString().split("/n");
        
            if (lines[0].contains("Romance") || lines[0].contains("Action") || lines[0].contains("Comedy"))
            {
	
                String[] words = lines[0].split(";");
                if(words[1].equals("movie") || words[1].equals("tvMovie") || words[1].equals("tvSpecial"))
                {
        	        if (!words[4].equals("\\N"))
        	        {
        		            context.write(new Text(words[0]), new Text("title:" + words[1]+","+words[2]+","+words[4]));
        	        }
                }
            }
        }
    }
	public static class ActorMapper extends Mapper<LongWritable, Text, Text, Text> 
    {
        public void map(LongWritable k, Text value, Context context) throws IOException, InterruptedException 
        {

            String[] words = value.toString().split(";");
            if(!words[2].equals("\\N"))
            {
        	    context.write(new Text(words[0]), new Text("dept:" + words[1]));
            }
        }
    }

    public static class DirectorMapper extends Mapper<LongWritable, Text, Text, Text> 
    {
        public void map(LongWritable k, Text value, Context context) throws IOException, InterruptedException 
        {

            String[] words = value.toString().split(";");
            if(!words[1].equals("\\N"))
            {
        	    context.write(new Text(words[0]), new Text("dir:" + words[1]));
            }
        }
    }
	
	public static class FirstReducer extends Reducer<Text, Text, NullWritable, Text> 
    {
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
    {

        String name = "";
        String dept = "";
        String dir = "";
        String checker = "";
        for(Text value : values) 
        {
        
            if (value.toString().startsWith("title")) 
            {
                name = value.toString().split(":")[1];
            }
            else if (value.toString().startsWith("dir")) 
            {
            	dir = value.toString().split(":")[1];
            } 
            else 
            {
            	if(!dept.equals(""))
            	{
            		checker = value.toString().split(":")[1];
            		dept = dept+","+checker;
            	}
            	else
            	{
                dept = value.toString().split(":")[1];
                }
            }
        }
        
        if(!dept.equals("") && !name.equals("") && !dir.equals(""))
        {
        	if(dept.contains(dir))
        	{
		        String merge = key + ","+ name + "," + dept + ","+ dir;
		        context.write(NullWritable.get(), new Text(merge));
        	}
        }
    }
}

    public static class IntermediateMapper extends Mapper<Object, Text, Text, IntWritable>
    {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
        {
      
            StringTokenizer itr = new StringTokenizer(value.toString(), ",");
            while (itr.hasMoreTokens()) 
            {
                String str = itr.nextToken();
                word.set(str);
                if(str.equals("movie") || str.equals("tvMovie") || str.equals("tvSpecial"))
                {
                    context.write(word, one);
                }
            }
        }
    
    }
  
  public static class FinalCount extends Reducer<Text,IntWritable,Text,IntWritable> 
  {
    private IntWritable result = new IntWritable();
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException 
    {
        int sum = 0;
        for (IntWritable val : values) 
        {
            sum += val.get();
        }
        result.set(sum);
        context.write(key, result);  
    }
  }
	
	public static void main(final String[] args) throws Exception 
	{
    		Configuration conf = new Configuration();
    		Job job = new Job(conf, "Actor-Director gig");
    		job.setJarByClass(IMDBA.class);
    		job.setReducerClass(FirstReducer.class);

    		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TitleMapper.class);

    		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, ActorMapper.class);
    		
    		MultipleInputs.addInputPath(job, new Path(args[2]), TextInputFormat.class, JoinMapperDirector.class);

    		job.setMapOutputKeyClass(Text.class);
    		job.setMapOutputValueClass(Text.class);
    		job.setOutputKeyClass(Text.class);
    		job.setOutputValueClass(Text.class);
    		job.setOutputFormatClass(TextOutputFormat.class);
    		FileOutputFormat.setOutputPath(job, new Path(args[3]+"inter"));
    		job.waitForCompletion(true);
    		
    		Job job1 = new Job(conf, "Actor-Director gig");
    		job1.setJarByClass(IMDBA.class);
    		job1.setMapperClass(IntermediateMapper.class);
    		job1.setReducerClass(FinalCount.class);
    		job1.setMapOutputKeyClass(Text.class);
    		job1.setOutputKeyClass(Text.class);
    		job1.setOutputValueClass(IntWritable.class);
    		FileInputFormat.addInputPath(job1, new Path(args[3]+"inter"));
    		FileOutputFormat.setOutputPath(job1, new Path(args[3]));
    		System.exit(job1.waitForCompletion(true) ? 0 : 1);
	}
	
}
