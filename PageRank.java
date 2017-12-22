//Gowtham Kommineni
//gkommine@uncc.edu

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


public class PageRank extends Configured implements Tool {

   private static final Logger LOG = Logger .getLogger( PageRank.class);

   public static void main( String[] args) throws  Exception {  
	      int res  = ToolRunner .run( new PageRank(), args);
	      System.exit(res);
   }

public int run( String[] args) throws  Exception {
	   Job job;
	  
	   for(int i=1;i<=10;i++)
	   { 
		  Configuration c=getConf();
		  job  = Job .getInstance(getConf(), " wordcount ");
	      job.setJarByClass( this .getClass());
	      
	      //Deleting intermediate folders created by page rank and Link Graph
	      FileSystem file=FileSystem.get(c);
	      if(i==2){
	    	 Path p=new Path(args[0]);
	    	 if(file.exists(p)){
	    		 file.delete(p,true);
	    	 }
	      }
	      if(i>2){
	    	  Path pa=new Path(args[1]+'/'+(i-2));
	    	  if(file.exists(pa)){
		    		 file.delete(pa,true);
		    	 }
	    	  
	      } 
	      
	      //Setting intermediate paths 
	      String ip_path=(i==1)?args[0]:args[1]+'/'+(i-1);
	      String op_path=(i==10)?args[1]+'/'+"final": args[1]+'/'+i;
	      
	      FileInputFormat.addInputPaths(job,  ip_path);
	      FileOutputFormat.setOutputPath(job,  new Path(op_path));
	      job.setMapperClass( Map .class);
	      job.setReducerClass( Reduce .class);
	      job.setOutputKeyClass( Text .class);
	      job.setOutputValueClass( Text .class);
	      job.waitForCompletion(true);
	  }
      return 1; 
   }
   
   public static class Map extends Mapper<LongWritable ,  Text ,  Text, Text> {
      private final static IntWritable one  = new IntWritable( 1);
      private Text word  = new Text();

      public void map( LongWritable offset,  Text lineText,  Context context)
        throws  IOException,  InterruptedException {

         String line  = lineText.toString();
         Text currentWord  = new Text();
         
         String[] input=line.split("\\t");
         String[] links=input[0].split("#####"); //this includes the node and outgoing links
         
         if(links.length>1){     //checks for outgoing links
        	 context.write(new Text(links[0]), new Text('>'+input[0]) ); //setting an identifier for node
             
	         for(int i=1;i<links.length;i++){ //loops over out links
	        	 try{
	        	 Double curr_rank= Double.parseDouble(input[1])/(links.length-1); //Pagerank / num of out link
	             context.write(new Text(links[i]),new Text(""+curr_rank)); //emits each out link 
	        	 }catch(Throwable e){System.out.println(e);;}
	         	}
         }
         }            
         }

   public static class Reduce extends Reducer<Text ,  Text ,  Text ,  Text > {
      @Override 
      public void reduce( Text link, Iterable<Text> values,  Context context)
         throws IOException,  InterruptedException {
    	  
    	  Double new_rank=0.0;
    	  String outLinks=""; //for saving node and its out links
    	  Boolean hasLinks=false;
    	  
    	  for(Text value:values){
    		  if(value.toString().charAt(0)=='>'){ //checks for node
    			  outLinks=value.toString();
    			  hasLinks=true;
    			  continue;
    		  }
    		  new_rank+=Double.parseDouble(value.toString());
    	  }
    	  if(!hasLinks) return; // checks if a node has no out links 
    	  
    	  new_rank=0.15+0.85*new_rank;
    	  
    	  if(!outLinks.equals("")){
    		  context.write(new Text(outLinks.substring(1,outLinks.length())), new Text(""+new_rank));  
    	  }
    	  else{
    		  context.write(link, new Text(""+new_rank));
    	  }
    	 }  
   }
}


