//Gowtham Kommineni
//gkommine@uncc.edu

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


public class LinkGraphGen extends Configured implements Tool {

   private static final Logger LOG = Logger .getLogger( LinkGraphGen.class);

   public static void main( String[] args) throws  Exception {
      int res  = ToolRunner .run( new LinkGraphGen(), args);
      System .exit(res);
   }

   public int run( String[] args) throws  Exception {
      Job job  = Job .getInstance(getConf(), " wordcount ");
      job.setJarByClass( this .getClass());

      FileInputFormat.addInputPaths(job,  args[0]);
      FileOutputFormat.setOutputPath(job,  new Path(args[ 1]));
      job.setMapperClass( Map .class);
      job.setReducerClass( Reduce .class);
      job.setOutputKeyClass( IntWritable .class);
      job.setOutputValueClass( Text .class);

      return job.waitForCompletion( true)  ? 0 : 1;
   }
   
   public static class Map extends Mapper<LongWritable ,  Text ,  IntWritable, Text> {
      private final static IntWritable one  = new IntWritable( 1);
      private Text word  = new Text();

      public void map( LongWritable offset,  Text lineText,  Context context)
        throws  IOException,  InterruptedException {

         String line  = lineText.toString();
         Text currentWord  = new Text();
                  
         Pattern linkPat = Pattern .compile("\\[\\[.*?]\\]");
         Pattern titlepat= Pattern. compile("<title>(.+?)</title>");

         Matcher m = linkPat.matcher(line); // extract outgoing links
         Matcher titleMat=titlepat.matcher(line);
    	 
         while(titleMat.find()) { // loop on each title
	         String link = titleMat.group(1); // drop the brackets and any nested ones if any
	         while(m.find()){      // loop on each outgoing link
	        	 String url= m.group().replace("[[", "").replace("]]", "");
	        	 if(!url.isEmpty()){
	        		 link+="#####"+url;	        	 
	        		 }
	         }
        	 currentWord  = new Text(link);
             context.write(one, currentWord); //emits one as key for finding N 
	         }
         }            
         }

   public static class Reduce extends Reducer<IntWritable ,  Text ,  Text ,  Text > {
      @Override 
      public void reduce( IntWritable count, Iterable<Text> words,  Context context)
         throws IOException,  InterruptedException {
         Double N  = 0.0;
         
         List<String> links= new ArrayList<String>();  
         
         for ( Text word  : words) {
        	 N += 1;
        	 links.add(word.toString());  	 
         }
         
         //Initializing page rank for each node
         for( String link: links){
        	 context.write(new Text(link),new Text(""+1.0/N));
         }
         }
   }
}

