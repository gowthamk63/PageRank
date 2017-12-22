# Page Rank

## Execution Instructions:
### Environment: Cloudera
Create input folder and move all the input files into it

### Run the MR job with :
Give input and output paths as arguments.
```
hadoop jar <jar_name> Sorting <input_path> <output_path>
```
All output files are created inside subfolders of the output folder.
Delete the ouput folder before re-executing the job.
### Output format for node with two outgoing links would be:
```
   a) LinkGraph: <link>#####<outlink1>#####<outllink2> <initial_page_rank=1/N>
   b) PageRank: <link>#####<outlink1>#####<outlink2> <page_rank>
   c) Sort: <link> <page_rank>
```
Output.txt contains top 100 links for the simple-wiki file.
