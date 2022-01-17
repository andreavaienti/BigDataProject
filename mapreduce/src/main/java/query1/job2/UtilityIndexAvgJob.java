package query1.job2;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import utils.TripleValue;
import utils.TuplaValue;

import java.io.IOException;

/**
 * MapReduce job to evaluate the User Utility Index Average for each Brand.
 */
public class UtilityIndexAvgJob {

    /**
     * Mapper for job2
     */
    public static class UtilityIndexAvgMapper extends Mapper<
            LongWritable, Text,
            TuplaValue<Text, Text>, TuplaValue<IntWritable, IntWritable>> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            //INPUT: (prodID, (brand, revID, vote)
            final String[] metaAttributes = value.toString().split(",", -1);
            final String prodID = metaAttributes[0].trim();
            final String brand = metaAttributes[1].trim();
            final String revID = metaAttributes[0].trim();
            final String vote = metaAttributes[1].trim();

            //OUTPUT: ((brand, revID), (vote, 1))
            context.write(new TuplaValue<Text, Text>(new Text(brand), new Text(revID)), new TuplaValue<IntWritable, IntWritable>(new IntWritable(Integer.parseInt(vote)), new IntWritable(1)));

        }
    }

    /**
     * Combiner
     */
    public static class UtilityIndexAvgCombiner extends Reducer<
            TuplaValue<Text, Text>, TuplaValue<IntWritable, IntWritable>,
            TuplaValue<Text, Text>, TuplaValue<IntWritable, IntWritable>> {

        public void reduce(TuplaValue<Text, Text> key, Iterable<TuplaValue<IntWritable, IntWritable>> values, Context context) throws IOException, InterruptedException {

            int sumLocalVote = 0, sumLocalCount = 0;

            for(TuplaValue<IntWritable, IntWritable> val: values){
                sumLocalVote += val.getLeft().get();
                sumLocalCount += val.getRight().get();
            }

            context.write(key, new TuplaValue<IntWritable, IntWritable>(new IntWritable(sumLocalVote), new IntWritable(sumLocalCount)));

        }

    }

    /**
     * Reducer
     */
    public static class UtilityIndexAvgReducer extends Reducer<
            TuplaValue<Text, Text>, TuplaValue<IntWritable, IntWritable>,
            TuplaValue<Text, Text>, DoubleWritable> {

        public void reduce(TuplaValue<Text, Text> key, Iterable<TuplaValue<IntWritable, IntWritable>> values, Context context) throws IOException, InterruptedException {

            int sumVote = 0, sumCount = 0;

            for(TuplaValue<IntWritable, IntWritable> val: values){
                sumVote += val.getLeft().get();
                sumCount += val.getLeft().get();
            }

            System.out.println("REDUCER22222222222222222222222222222222222222222222222");
            System.out.println("K: " + key.toString() + "  , V: " + sumVote/sumCount);
            //OUTPUT: ((brand, revID), utilityIndex)
            context.write(key, new DoubleWritable(sumVote/sumCount));

        }

    }
}
