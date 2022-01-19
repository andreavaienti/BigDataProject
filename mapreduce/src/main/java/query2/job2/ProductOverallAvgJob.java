package query2.job2;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import utils.parser.CoreRecordParser;
import utils.tuplaValue.IntIntTuplaValue;

import java.io.IOException;

public class ProductOverallAvgJob {

    /**
     * Mapper for job2
     */
    public static class ProductOverallAvgMapper extends Mapper<
            LongWritable, Text,
            Text, IntIntTuplaValue> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            if(CoreRecordParser.areParsable(value.toString())) {
                //INPUT: leggo dataset
                final String[] coreAttributes = value.toString().split(",", -1);
                final String prodID = coreAttributes[3].trim() + ",";
                final String overall = coreAttributes[0].trim();

                //OUTPUT: ((prodID), overall, 1)
                context.write(new Text(prodID), new IntIntTuplaValue(new IntWritable(Integer.parseInt(overall)), new IntWritable(1)));
            }
        }
    }

    /**
     * Combiner
     */
    public static class ProductOverallAvgCombiner extends Reducer<
            Text, IntIntTuplaValue,
            Text, IntIntTuplaValue> {

        public void reduce(Text key, Iterable<IntIntTuplaValue> values, Context context) throws IOException, InterruptedException {

            int sumLocalOverall = 0, sumLocalItem = 0;

            for(IntIntTuplaValue val: values){
                sumLocalOverall += val.getLeft().get();
                sumLocalItem += val.getRight().get();
            }

            context.write(key, new IntIntTuplaValue(new IntWritable(sumLocalOverall), new IntWritable(sumLocalItem)));
        }

    }

    /**
     * Reducer
     */
    public static class ProductOverallAvgReducer extends Reducer<
            Text, IntIntTuplaValue,
            Text, DoubleWritable> {

        public void reduce(Text key, Iterable<IntIntTuplaValue> values, Context context) throws IOException, InterruptedException {

            double sumTotalOverall = 0.0, sumTotalItem = 0.0;

            for(IntIntTuplaValue val: values){
                sumTotalOverall += val.getLeft().get();
                sumTotalItem += val.getRight().get();
            }

            //OUTPUT: ((prodID), overall)
            context.write(key, new DoubleWritable(sumTotalOverall/sumTotalItem));

        }

    }
}
