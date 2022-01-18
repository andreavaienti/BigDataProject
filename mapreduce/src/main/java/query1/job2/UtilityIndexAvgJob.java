package query1.job2;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import utils.IntIntTuplaValue;
import utils.TextTextTuplaValue;
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
            TextTextTuplaValue, IntIntTuplaValue> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            //INPUT: (prodID, (brand, revID, vote)
            final String[] metaAttributes = value.toString().split(",", -1);
            final String brand = metaAttributes[1].trim();
            final String revID = metaAttributes[2].trim();
            final String vote = metaAttributes[3].trim();

            //OUTPUT: ((brand, revID), (vote, 1))
            context.write(new TextTextTuplaValue(new Text(brand), new Text(revID)), new IntIntTuplaValue(new IntWritable(Integer.parseInt(vote)), new IntWritable(1)));

        }
    }

    /**
     * Combiner
     */
    public static class UtilityIndexAvgCombiner extends Reducer<
            TextTextTuplaValue, IntIntTuplaValue,
            TextTextTuplaValue, IntIntTuplaValue> {

        public void reduce(TextTextTuplaValue key, Iterable<IntIntTuplaValue> values, Context context) throws IOException, InterruptedException {

            int sumLocalVote = 0, sumLocalCount = 0;

            for(IntIntTuplaValue val: values){
                sumLocalVote += val.getLeft().get();
                sumLocalCount += val.getRight().get();
            }

            context.write(key, new IntIntTuplaValue(new IntWritable(sumLocalVote), new IntWritable(sumLocalCount)));

        }

    }

    /**
     * Reducer
     */
    public static class UtilityIndexAvgReducer extends Reducer<
            TextTextTuplaValue, IntIntTuplaValue,
            TextTextTuplaValue, DoubleWritable> {

        public void reduce(TextTextTuplaValue key, Iterable<IntIntTuplaValue> values, Context context) throws IOException, InterruptedException {

            double sumVote = 0.0, sumCount = 0.0;

            for(IntIntTuplaValue val: values){
                sumVote += val.getLeft().get();
                sumCount += val.getRight().get();
            }

            //OUTPUT: ((brand, revID), utilityIndex)
            context.write(key, new DoubleWritable(sumVote/sumCount));

        }
    }
}
