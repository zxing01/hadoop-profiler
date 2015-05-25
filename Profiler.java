import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Profiler {
    
    public static class TokenizerMapper
    extends Mapper<Object, Text, Text, LongWritable>{
        
        private Text outKey = new Text();
        private LongWritable outValue = new LongWritable();
        private String token;
        private String[] pair;
        private long v;

        public void map(Object key, Text value, Context context
                        ) throws IOException, InterruptedException {
            
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                try {
                token = itr.nextToken();
                pair = token.split(",");
                //for (int i = 0; i < pair.length; ++i) {
                    //System.out.printf("@@@@@ pair[%d] = %s\n", i, pair[i]);
                //}
                outKey.set(pair[0]);
                String outStr = "";
                for(int i = 0;i < pair[1].length(); i++) {
                    char ch = pair[1].charAt(i);
                    if( Character.isDigit(ch)) {
                        outStr+=ch;
                    }
                }
                outValue.set(Long.parseLong(outStr));
                }
                catch (Exception e) {
                    System.out.println(e.toString());
                    //System.out.println("@@@@@ pair[1] = " + pair[1] + " length = " + pair[1].length());
                }
                context.write(outKey, outValue);
            }

        }
    }
    
    public static class AvgReducer
    extends Reducer<Text,LongWritable,Text,LongWritable> {
        private LongWritable result = new LongWritable();
        
        public void reduce(Text key, Iterable<LongWritable> values,
                           Context context
                           ) throws IOException, InterruptedException {
            try {
            int sum = 0;
            int count = 0;
            for (LongWritable val : values) {
                sum += val.get();
                ++count;
            }
            result.set(sum/count);
            context.write(key, result);
            }
            catch (Exception e) {
                System.out.println(e.getMessage());
            }
        }
    }
    
    public static void main(String[] args) throws Exception {
        
        if (args.length < 2) {
            System.out.println("Profiler <inDir> <outDir>");
            ToolRunner.printGenericCommandUsage(System.out);
            System.exit(1);
        }
        
        Path tempDir =
        new Path("profiler-temp-"+
                 Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));
        
        Configuration conf = new Configuration();
        
        try {
            //Configuration conf = getConf();
            Job avgJob = Job.getInstance(conf, "profiler-avg");
            avgJob.setJarByClass(Profiler.class);
            FileInputFormat.addInputPath(avgJob, new Path(args[0]));
        
            avgJob.setMapperClass(TokenizerMapper.class);
            //avgJob.setCombinerClass(AvgReducer.class);
            avgJob.setReducerClass(AvgReducer.class);
        
            FileOutputFormat.setOutputPath(avgJob, tempDir);
            avgJob.setOutputFormatClass(SequenceFileOutputFormat.class);
            avgJob.setOutputKeyClass(Text.class);
            avgJob.setOutputValueClass(LongWritable.class);
            avgJob.waitForCompletion(true);
        
            Job sortJob = Job.getInstance(conf, "profiler-sort");
            FileInputFormat.setInputPaths(sortJob, tempDir);
            sortJob.setInputFormatClass(SequenceFileInputFormat.class);
            
            sortJob.setMapperClass(InverseMapper.class);
            sortJob.setNumReduceTasks(1);                 // write a single file
        
            FileOutputFormat.setOutputPath(sortJob, new Path(args[1]));
            sortJob.setSortComparatorClass(          // sort by decreasing freq
                                       LongWritable.DecreasingComparator.class);
        
            sortJob.waitForCompletion(true);
        }
        catch (Exception e) {
            System.out.println(e.toString());
        }
        finally {
            FileSystem.get(conf).delete(tempDir, true);
        }
        System.exit(0);
    }
}