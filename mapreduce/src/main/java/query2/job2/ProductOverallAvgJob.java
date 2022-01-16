package query2.job2;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import utils.TripleValue;
import utils.TuplaValue;

import java.io.IOException;

public class ProductOverallAvgJob {

    /**
     * Mapper for job2
     */
    public static class ProductOverallAvgMapper extends Mapper<
            IntWritable, Text,
            Text, TuplaValue<IntWritable, IntWritable>> {

        public void map(IntWritable key, Text value, Context context) throws IOException, InterruptedException {

            final String[] coreAttributes = value.toString().split(",", -1);
            final String prodID = coreAttributes[2].trim();
            final String overall = coreAttributes[0].trim();

            //INPUT: leggo dataset
            //OUTPUT: ((prodID), overall, 1)
            context.write(new Text(prodID), new TuplaValue<IntWritable, IntWritable>(new IntWritable(Integer.parseInt(overall)), new IntWritable(1)));

        }
    }

    /**
     * Combiner
     */
    public static class ProductOverallAvgCombiner extends Reducer<
            Text, TuplaValue<IntWritable, IntWritable>,
            Text, TuplaValue<IntWritable, IntWritable>> {

        public void reduce(Text key, Iterable<TuplaValue<IntWritable, IntWritable>> values, Context context) throws IOException, InterruptedException {

            int sumLocalOverall = 0, sumLocalItem = 0;

            for(TuplaValue<IntWritable, IntWritable> val: values){
                sumLocalOverall += val.getLeft().get();
                sumLocalItem += val.getRight().get();
            }

            context.write(key, new TuplaValue<IntWritable, IntWritable>(new IntWritable(sumLocalOverall), new IntWritable(sumLocalItem)));

        }

    }

    /**
     * Reducer
     */
    public static class ProductOverallAvgReducer extends Reducer<
            Text, TuplaValue<IntWritable, IntWritable>,
            Text, DoubleWritable> {

        public void reduce(Text key, Iterable<TuplaValue<IntWritable, IntWritable>> values, Context context) throws IOException, InterruptedException {

            int sumTotalOverall = 0, sumTotalItem = 0;

            for(TuplaValue<IntWritable, IntWritable> val: values){
                sumTotalOverall += val.getLeft().get();
                sumTotalItem += val.getLeft().get();
            }

            //OUTPUT: ((brand, revID), utilityIndex)
            context.write(key, new DoubleWritable(sumTotalOverall/sumTotalItem));

        }

    }
}
