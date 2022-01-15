package query1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import query1.job1.MetaAndCoreJoinJob;

import java.io.File;

public class QueryMR {

	public static void main(final String[] args) throws Exception {

		if (args.length < 2) {
			System.out.println("Parameters required: <input dir> <output dir> " +
					"[OPTIONAL <num reducers job 1>] [OPTIONAL <num reducers job 2>]");
			System.exit(-1);
		}

		final Path inputPath = new Path(args[0]);
		final Path outputPath = new Path(args[1]);
		final Path fiveCoreDatasetPath = new Path(inputPath + File.separator + "5-core-sample.csv");
		final Path metadataDatasetPath = new Path(inputPath + File.separator + "meta-sample.csv");
		final Path job1Result = new Path(outputPath + File.separator + "job1Result");

		final FileSystem fs = FileSystem.get(new Configuration());
		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}

		/*
		 * First job
		 */
		final Configuration conf1 = new Configuration();
		final Job job1 = Job.getInstance(conf1, "Compute most upvoted answer for each question");
		job1.setJarByClass(QueryMR.class);

		if (args.length > 2) {
			final int numReduceTasksJob1 = Integer.parseInt(args[2]);
			if (numReduceTasksJob1 >= 0) {
				job1.setNumReduceTasks(numReduceTasksJob1);
			}
		} else {
			job1.setNumReduceTasks(1);
		}

		//job1.setMapperClass(MetaAndCoreJoinJob.class);
		MultipleInputs.addInputPath(job1, metadataDatasetPath, KeyValueTextInputFormat.class, MetaAndCoreJoinJob.MetaMapper.class);
		MultipleInputs.addInputPath(job1, fiveCoreDatasetPath, KeyValueTextInputFormat.class, MetaAndCoreJoinJob.CoreMapper.class);

		job1.setReducerClass(MetaAndCoreJoinJob.JobReducer.class);
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