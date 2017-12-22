Gowtham Kommineni
gkommine@uncc.edu


Execution Instructions:
 
1. Run the MR job with : hadoop jar <jar_name> Sorting <input_path> <output_path>
2. All output files are created inside subfolders of the output folder.
3. Delete the ouput folder before re-executing the job.
4. Output format for node with two outgoing links would be:
   a) LinkGraph: <link>#####<outlink1>#####<outllink2> <initial_page_rank=1/N>
   b) PageRank: <link>#####<outlink1>#####<outlink2> <page_rank>
   c) Sort: <link> <page_rank>
5. Output.txt contains top 100 links for the simple-wiki file.
