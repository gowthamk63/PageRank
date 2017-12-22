//Gowtham Kommineni
//gkommine@uncc.edu

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.FileSystem;


public class Sorting extends Configured implements Tool {

   private static final Logger LOG = Logger .getLogger( Sorting.class);

   public static void main( String[] args) throws  Exception {
	  //Giving input and output folders for 
	  String[] LG_args={args[0],args[1]+"/LinkGraph"}; 
	  String[] PR_args={args[1]+"/LinkGraph",args[1]+"/PageRank"};
	  String[] Sort_args={args[1]+"/PageRank/final",args[1]+"/Sort"};
	  
	  int res2 = ToolRunner .run( new LinkGraphGen(),LG_args);
	  int res1 = ToolRunner .run(new PageRank(), PR_args);
      int res  = ToolRunner .run( new Sorting(), Sort_args);
      
      System .exit(res);
      System .exit(res1);
      System .exit(res2);
   }

   public int run( String[] args) throws  Exception {
      Job job  = Job .getInstance(getConf(), " Sorting ");
      Configuration c = getConf();
      
      job.setJarByClass( this .getClass());
      job.setNumReduceTasks(1);
      FileInputFormat.addInputPaths(job, args[0]);
      FileOutputFormat.setOutputPath(job,  new Path(args[1]));  
      
      job.setMapperClass( Map .class);
      job.setReducerClass( Reduce .class);
      job.setOutputKeyClass( DoubleWritable .class);
      job.setOutputValueClass( Text .class);
      
      job.waitForCompletion( true);
      //Deleting intermediate folders created by page rank
      FileSystem file=FileSystem.get(c);
	  Path p=new Path(args[0].replace("/final", ""));
	  if(file.exists(p)){
		  file.delete(p,true);
	  }
	  
      return 1;
   }
   
   public static class Map extends Mapper<LongWritable ,  Text ,  DoubleWritable, Text> {
	   
      public void map( LongWritable offset,  Text lineText,  Context context)
        throws  IOException,  InterruptedException {
    	  
         String line  = lineText.toString();
         
         //extracting node and its page rank
         String page= line.split("#####")[0]; 
         Double val = Double.parseDouble(line.split("\\t")[1]);
         
        //negating the value so that reducer sorts in descending order
        context.write(new DoubleWritable(-val), new Text(page));
      }
   }

   public static class Reduce extends Reducer<DoubleWritable , Text,  Text ,  Text > {
      @Override 
      public void reduce( DoubleWritable ranks,  Iterable<Text> pages,  Context context)
         throws IOException,  InterruptedException {
    	  
    	  Double val = ranks.get();
    	  
    	  for(Text name: pages){
    		  //Printing the negative value to remove the minus sign added at mapper
        	  context.write(new Text(name), new Text(Double.toString(-val)));
    	  }
      }
   }
}