package query1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import query1.job1.MetaAndCoreJoinJob;
import query1.job2.UtilityIndexAvgJob;
import query1.job3.UtilityIndexSortJob;
import utils.tripleValue.TripleValue;
import utils.tuplaValue.IntIntTuplaValue;
import utils.tuplaValue.TextDoubleTuplaValue;
import utils.tuplaValue.TextTextTuplaValue;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Query1MR {

	public static void main(final String[] args) throws Exception {

		List<Integer> numReduceTasksForJobs = new ArrayList<Integer>(Arrays.asList(1, 1, 1));

		if (args.length < 2) {
			System.out.println("Parameters required: <input dir> <output dir> " +
					"[OPTIONAL <num reducers job 1>] [OPTIONAL <num reducers job 2>]");
			System.exit(-1);
		}

		for(int i = 2; i < args.length; i++){
			numReduceTasksForJobs.set(i, Integer.parseInt(args[i]));
		}

		final Path inputPath = new Path(args[0]);
		final Path outputPath = new Path(args[1]);
		//final Path fiveCoreDatasetPath = new Path(inputPath + File.separator + "5-core-sample.csv");
		//final Path metadataDatasetPath = new Path(inputPath + File.separator + "meta-sample.csv");
		final Path fiveCoreDatasetPath = new Path(inputPath + File.separator + "coreWithError.csv");
		final Path metadataDatasetPath = new Path(inputPath + File.separator + "metaWithError.csv");
		final Path job1Result = new Path(outputPath + File.separator + "job1Result");
		final Path job2Result = new Path(outputPath + File.separator + "job2Result");
		final Path job3Result = new Path(outputPath + File.separator + "job3Result");

		final FileSystem fs = FileSystem.get(new Configuration());
		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}

		/*
		 * First job
		 */
		final Configuration conf1 = new Configuration();
		final Job job1 = Job.getInstance(conf1, "Meta and Core Dataset Join");
		job1.setJarByClass(Query1MR.class);
		job1.setNumReduceTasks(numReduceTasksForJobs.get(0));

		job1.setMapperClass(MetaAndCoreJoinJob.MetaMapper.class);
		job1.setMapperClass(MetaAndCoreJoinJob.CoreMapper.class);
		job1.setReducerClass(MetaAndCoreJoinJob.JobReducer.class);

		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(TripleValue.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(TripleValue.class);

		MultipleInputs.addInputPath(job1, metadataDatasetPath, TextInputFormat.class, MetaAndCoreJoinJob.MetaMapper.class);
		MultipleInputs.addInputPath(job1, fiveCoreDatasetPath, TextInputFormat.class, MetaAndCoreJoinJob.CoreMapper.class);
		FileOutputFormat.setOutputPath(job1, job1Result);

		if (!job1.waitForCompletion(true)) {
			System.exit(1);
		}

		/*
		 * Second job
		 */
		final Configuration conf2 = new Configuration();
		final Job job2 = Job.getInstance(conf2, "Utility Index Average Job");
		job2.setJarByClass(Query1MR.class);
		job2.setNumReduceTasks(numReduceTasksForJobs.get(1));

		job2.setMapperClass(UtilityIndexAvgJob.UtilityIndexAvgMapper.class);
		job2.setCombinerClass(UtilityIndexAvgJob.UtilityIndexAvgCombiner.class);
		job2.setReducerClass(UtilityIndexAvgJob.UtilityIndexAvgReducer.class);

		job2.setMapOutputKeyClass(TextTextTuplaValue.class);
		job2.setMapOutputValueClass(IntIntTuplaValue.class);
		job2.setOutputKeyClass(TextTextTuplaValue.class);
		job2.setOutputValueClass(DoubleWritable.class);

		FileInputFormat.addInputPath(job2, job1Result);
		FileOutputFormat.setOutputPath(job2, job2Result);

		if (!job2.waitForCompletion(true)) {
			System.exit(1);
		}

		/*
		 * Third job
		 */
		final Configuration conf3 = new Configuration();
		final Job job3 = Job.getInstance(conf3, "Utility Index Sort Job");
		job3.setJarByClass(Query1MR.class);
		job3.setNumReduceTasks(numReduceTasksForJobs.get(2));

		job3.setMapperClass(UtilityIndexSortJob.UtilityIndexSortMapper.class);
		job3.setReducerClass(UtilityIndexSortJob.UtilityIndexSortReducer.class);

		job3.setMapOutputKeyClass(TextDoubleTuplaValue.class);
		job3.setMapOutputValueClass(Text.class);
		job3.setOutputKeyClass(TextDoubleTuplaValue.class);
		job3.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job3, job2Result);
		FileOutputFormat.setOutputPath(job3, job3Result);

		System.exit(job3.waitForCompletion(true) ? 0 : 1);
	}
}