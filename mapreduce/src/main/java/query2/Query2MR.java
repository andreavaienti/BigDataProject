package query2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import query2.job1.BrandWith3OrMoreProductFilterJob;
import query2.job2.ProductOverallAvgJob;
import query2.job3.BrandAndProductOverallJoinJob;
import query2.job4.BrandOverallAvgJob;
import query2.job5.BrandOverallFindMaxJob;
import utils.tuplaValue.*;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Query2MR {

    public static void main(final String[] args) throws Exception {

        List<Integer> numReduceTasksForJobs = new ArrayList<Integer>(Arrays.asList(1, 1, 1, 1, 1));

        if (args.length < 2) {
            System.out.println("Parameters required: <input dir> <output dir> " +
                    "[OPTIONAL <num reducers job 1>] ... [OPTIONAL <num reducers job 5>]");
            System.exit(-1);
        }

        for (int i = 2; i < args.length; i++) {
            numReduceTasksForJobs.set(i, Integer.parseInt(args[i]));
        }

        final Path inputPath = new Path(args[0]);
        final Path outputPath = new Path(args[1]);
        //final Path fiveCoreDatasetPath = new Path(inputPath + File.separator + "5-core-sample.csv");
        //final Path metadataDatasetPath = new Path(inputPath + File.separator + "meta-sample.csv");
        final Path fiveCoreDatasetPath = new Path(inputPath + File.separator + "core.csv");
        final Path metadataDatasetPath = new Path(inputPath + File.separator + "brand.csv");
        final Path job1Result = new Path(outputPath + File.separator + "job1Result");
        final Path job2Result = new Path(outputPath + File.separator + "job2Result");
        final Path job3Result = new Path(outputPath + File.separator + "job3Result");
        final Path job4Result = new Path(outputPath + File.separator + "job4Result");
        final Path job5Result = new Path(outputPath + File.separator + "job5Result");

        final FileSystem fs = FileSystem.get(new Configuration());
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        /*
         * First job
         */
        final Configuration conf1 = new Configuration();
        final Job job1 = Job.getInstance(conf1, "Brand with 2 or more product filter");
        job1.setJarByClass(Query2MR.class);
        job1.setNumReduceTasks(numReduceTasksForJobs.get(0));

        job1.setMapperClass(BrandWith3OrMoreProductFilterJob.Brand3ProductFilterMapper.class);
        job1.setReducerClass(BrandWith3OrMoreProductFilterJob.Brand3ProductFilterReducer.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(TextIntTuplaValue.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job1, metadataDatasetPath);
        FileOutputFormat.setOutputPath(job1, job1Result);

        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }

        /*
         * Second job
         */
        final Configuration conf2 = new Configuration();
        final Job job2 = Job.getInstance(conf2, "Product Overall Average Job");
        job2.setJarByClass(Query2MR.class);
        job2.setNumReduceTasks(numReduceTasksForJobs.get(1));

        job2.setMapperClass(ProductOverallAvgJob.ProductOverallAvgMapper.class);
        job2.setCombinerClass(ProductOverallAvgJob.ProductOverallAvgCombiner.class);
        job2.setReducerClass(ProductOverallAvgJob.ProductOverallAvgReducer.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(IntIntTuplaValue.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job2, fiveCoreDatasetPath);
        FileOutputFormat.setOutputPath(job2, job2Result);

        if (!job2.waitForCompletion(true)) {
            System.exit(1);
        }

        /*
         * Third job
         */
        final Configuration conf3 = new Configuration();
        final Job job3 = Job.getInstance(conf3, "Brand And Product Overall Join");
        job3.setJarByClass(Query2MR.class);
        job3.setNumReduceTasks(numReduceTasksForJobs.get(2));

        job3.setMapperClass(BrandAndProductOverallJoinJob.BrandMapper.class);
        job3.setMapperClass(BrandAndProductOverallJoinJob.ProductOverallMapper.class);
        job3.setReducerClass(BrandAndProductOverallJoinJob.JoinReducer.class);

        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(TextTextTuplaValue.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(TextTextTuplaValue.class);

        MultipleInputs.addInputPath(job3, job1Result, TextInputFormat.class, BrandAndProductOverallJoinJob.BrandMapper.class);
        MultipleInputs.addInputPath(job3, job2Result, TextInputFormat.class, BrandAndProductOverallJoinJob.ProductOverallMapper.class);
        FileOutputFormat.setOutputPath(job3, job3Result);

        if (!job3.waitForCompletion(true)) {
            System.exit(1);
        }

        /*
         * Fourth job
         */
        final Configuration conf4 = new Configuration();
        final Job job4 = Job.getInstance(conf4, "Product Overall Average Job");
        job4.setJarByClass(Query2MR.class);
        job4.setNumReduceTasks(numReduceTasksForJobs.get(3));

        job4.setMapperClass(BrandOverallAvgJob.BrandOverallAvgMapper.class);
        job4.setCombinerClass(BrandOverallAvgJob.BrandOverallAvgCombiner.class);
        job4.setReducerClass(BrandOverallAvgJob.BrandOverallAvgReducer.class);

        job4.setMapOutputKeyClass(Text.class);
        job4.setMapOutputValueClass(DoubleDoubleTuplaValue.class);
        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job4, job3Result);
        FileOutputFormat.setOutputPath(job4, job4Result);

        if (!job4.waitForCompletion(true)) {
            System.exit(1);
        }

        /*
         * Fifth job
         */
        final Configuration conf5 = new Configuration();
        final Job job5 = Job.getInstance(conf5, "Find Max Brand Overall Job");
        job5.setJarByClass(Query2MR.class);
        job5.setNumReduceTasks(numReduceTasksForJobs.get(4));

        job5.setMapperClass(BrandOverallFindMaxJob.BrandOverallMaxMapper.class);
        job5.setCombinerClass(BrandOverallFindMaxJob.BrandOverallMaxCombiner.class);
        job5.setReducerClass(BrandOverallFindMaxJob.BrandOverallMaxReducer.class);

        job5.setMapOutputKeyClass(IntWritable.class);
        job5.setMapOutputValueClass(TextDoubleTuplaValue.class);
        job5.setOutputKeyClass(DoubleWritable.class);
        job5.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job5, job4Result);
        FileOutputFormat.setOutputPath(job5, job5Result);

        System.exit(job5.waitForCompletion(true) ? 0 : 1);

    }
}
