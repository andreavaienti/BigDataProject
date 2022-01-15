package query1.job2;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
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
    public static class UtilityIndexAvgMapper extends Mapper<Text, TripleValue, TuplaValue<Text, Text>, TuplaValue<IntWritable, IntWritable>>{

        private IntWritable voteInt = new IntWritable();
        private IntWritable one = new IntWritable(1);

        public void map(Text key, TripleValue value, Context context) throws IOException, InterruptedException {

            //INPUT: (prodID, (brand, revID, vote)
            voteInt.set(Integer.parseInt(value.getRight().toString()));

            //OUTPUT: ((brand, revID), (vote, 1))
            context.write(new TuplaValue<Text, Text>(value.getLeft(), value.getCenter()), new TuplaValue<IntWritable, IntWritable>(voteInt, one));

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
                sumLocalCount += val.getLeft().get();
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

            //OUTPUT: ((brand, revID), utilityIndex)
            context.write(key, new DoubleWritable(sumVote/sumCount));

        }

    }
}
