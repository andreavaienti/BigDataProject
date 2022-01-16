package query2.job4;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import utils.TuplaValue;

import java.io.IOException;

public class BrandOverallAvgJob {

    /**
     * Mapper for job4
     */
    public static class BrandOverallAvgMapper extends Mapper<
            Text, TuplaValue<Text, Text>,
            Text, TuplaValue<IntWritable, IntWritable>> {

        public void map(Text key, TuplaValue<Text, Text> value, Context context) throws IOException, InterruptedException {

            //INPUT: (prodID, (brand, overall))
            //OUTPUT: (brand, overall, 1)
            context.write(value.getLeft(), new TuplaValue<IntWritable, IntWritable>(new IntWritable(Integer.parseInt(value.getRight().toString())), new IntWritable(1)));

        }
    }

    /**
     * Combiner
     */
    public static class BrandOverallAvgCombiner extends Reducer<
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
    public static class BrandOverallAvgReducer extends Reducer<
            Text, TuplaValue<IntWritable, IntWritable>,
            Text, DoubleWritable> {

        public void reduce(Text key, Iterable<TuplaValue<IntWritable, IntWritable>> values, Context context) throws IOException, InterruptedException {

            int sumTotalOverall = 0, sumTotalItem = 0;

            for(TuplaValue<IntWritable, IntWritable> val: values){
                sumTotalOverall += val.getLeft().get();
                sumTotalItem += val.getLeft().get();
            }

            //OUTPUT: ((prodID), overall)
            context.write(key,new DoubleWritable(sumTotalOverall/sumTotalItem));

        }

    }
}
