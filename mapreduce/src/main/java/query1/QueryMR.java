package query1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import query1.job1.MetaAndCoreJoinJob;
import query1.job2.UtilityIndexAvgJob;
import utils.TripleValue;
import utils.TuplaValue;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class QueryMR {

	public static void main(final String[] args) throws Exception {

		List<Integer> numReduceTasksForJobs = new ArrayList<Integer>(Arrays.asList(1, 1, 1, 1));

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
		final Path fiveCoreDatasetPath = new Path(inputPath + File.separator + "5-core-sample.csv");
		final Path metadataDatasetPath = new Path(inputPath + File.separator + "meta-sample.csv");
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
		job1.setJarByClass(QueryMR.class);
		job1.setNumReduceTasks(numReduceTasksForJobs.get(0));

		job1.setMapperClass(MetaAndCoreJoinJob.MetaMapper.class);
		job1.setMapperClass(MetaAndCoreJoinJob.CoreMapper.class);
		job1.setReducerClass(MetaAndCoreJoinJob.JobReducer.class);

		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(TripleValue.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(TripleValue.class);

		//job1.setMapperClass(MetaAndCoreJoinJob.class);
		MultipleInputs.addInputPath(job1, metadataDatasetPath, TextInputFormat.class, MetaAndCoreJoinJob.MetaMapper.class);
		MultipleInputs.addInputPath(job1, fiveCoreDatasetPath, TextInputFormat.class, MetaAndCoreJoinJob.CoreMapper.class);
		//FileInputFormat.addInputPath(job1, metadataDatasetPath);
		FileOutputFormat.setOutputPath(job1, job1Result);

		if (!job1.waitForCompletion(true)) {
			System.exit(1);
		}

		/*
		 * Second job
		 */
		final Configuration conf2 = new Configuration();
		final Job job2 = Job.getInstance(conf2, "Utility Index Average Job");
		job2.setJarByClass(QueryMR.class);
		job2.setNumReduceTasks(numReduceTasksForJobs.get(1));

		job2.setMapperClass(UtilityIndexAvgJob.UtilityIndexAvgMapper.class);
		job2.setCombinerClass(UtilityIndexAvgJob.UtilityIndexAvgCombiner.class);
		job2.setReducerClass(UtilityIndexAvgJob.UtilityIndexAvgReducer.class);

		job2.setMapOutputKeyClass(TuplaValue.class);
		job2.setMapOutputValueClass(TuplaValue.class);
		job2.setOutputKeyClass(TuplaValue.class);
		job2.setOutputValueClass(DoubleWritable.class);

		FileInputFormat.addInputPath(job2, job1Result);
		FileOutputFormat.setOutputPath(job2, job2Result);

		if (!job2.waitForCompletion(true)) {
			System.exit(1);
		}

		/*
		 * Third job
		 */
		/*final Configuration conf3 = new Configuration();
		final Job job3 = Job.getInstance(conf3, "Utility Index Sort Job");
		job3.setJarByClass(QueryMR.class);

		if (args.length > 2) {
			final int numReduceTasksJob2 = Integer.parseInt(args[4]);
			if (numReduceTasksJob3 >= 0) {
				job3.setNumReduceTasks(numReduceTasksJob3);
			}
		} else {
			job3.setNumReduceTasks(1);
		}

		job3.setMapperClass(UtilityIndexSortJob.UtilityIndexSortMapper.class);
		job3.setReducerClass(UtilityIndexSortJob.UtilityIndexSortReducer.class);
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job3, job2Result);
		FileOutputFormat.setOutputPath(job3, job3Result);

		if (!job3.waitForCompletion(true)) {
			System.exit(1);
		}

		/*
		 * Second job
		 */
		/*
		final Configuration conf2 = new Configuration();
		final FileStatus[] fileList = fs.listStatus(temp1Path, new PathFilter() {
			public boolean accept(final Path path) {
				return path.getName().contains("part-");
			}
		});
		for (final FileStatus file : fileList) {
			DistributedCache.addCacheFile(new URI(temp1Path + File.separator + file.getPath().getName()), conf2);
		}
		final Job job2 = Job.getInstance(conf2, "Join answers' upvotes and tags and compute average");
		job2.setJarByClass(QueryMR.class);

		if (args.length > 3) {
			final int numReduceTasksJob2 = Integer.parseInt(args[3]);
			if (numReduceTasksJob2 >= 0) {
				job2.setNumReduceTasks(numReduceTasksJob2);
			}
		} else {
			job2.setNumReduceTasks(1);
		}
		job2.setMapperClass(JoinAnswersTagsMapper.class);
		job2.setCombinerClass(PartialSumCountCombiner.class);
		job2.setReducerClass(AverageUpvotesForTagReducer.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(LongPairWritable.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(FloatWritable.class);

		FileInputFormat.addInputPath(job2, tagsPath);
		FileOutputFormat.setOutputPath(job2, temp2Path);

		if (!job2.waitForCompletion(true)) {
			System.exit(1);
		}

		/*
		 * Third job
		 */
		/*
		final Configuration conf3 = new Configuration();
		final Job job3 = Job.getInstance(conf3, "Compute tag leaderboard");
		job3.setJarByClass(QueryMR.class);

		job3.setNumReduceTasks(1);
		job3.setMapperClass(TagSorterMapper.class);
		job3.setReducerClass(LeaderboardReducer.class);
		job3.setMapOutputKeyClass(FloatWritable.class);
		job3.setMapOutputValueClass(Text.class);
		job3.setOutputKeyClass(LongWritable.class);
		job3.setOutputValueClass(Text.class);
		job3.setSortComparatorClass(FloatDescendingSortComparator.class);

		FileInputFormat.addInputPath(job3, temp2Path);
		FileOutputFormat.setOutputPath(job3, resultPath);

		System.exit(job3.waitForCompletion(true) ? 0 : 1);
		*/
	}
}