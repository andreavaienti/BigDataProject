package query2.job1;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import utils.TripleValue;
import utils.TuplaValue;

import java.io.IOException;

public class BrandWith3OrMoreProductFilterJob {

    /**
     * Mapper for job1
     */
    public static class Brand3ProductFilterMapper extends Mapper<
            IntWritable, Text,
            Text, TuplaValue<Text, IntWritable>> {

        public void map(IntWritable key, Text value, Context context) throws IOException, InterruptedException {

            final String[] metaAttributes = value.toString().split(",", -1);
            final String brand = metaAttributes[0].trim();
            final String prodID = metaAttributes[1].trim();

            //INPUT: Leggo file
            //OUTPUT: ((brand), prodID, 1)
            context.write(new Text(brand), new TuplaValue<Text, IntWritable>(new Text(prodID), new IntWritable(1)));

        }

    }

    /**
     * Reducer
     */
    public static class UtilityIndexSortReducer extends Reducer<
            Text, TuplaValue<Text, IntWritable>,
            Text, Text> {

        public void reduce(Text key, Iterable<TuplaValue<Text, IntWritable>> values, Context context) throws IOException, InterruptedException {

            //INPUT: ((brand), prodID, 1)
            int brandProductCounter = 0;

            for(TuplaValue<Text, IntWritable> val: values){
                brandProductCounter += val.getRight().get();
            }

            //OUTPUT: ((brand), prodID)
            if(brandProductCounter >= 3){
                for(TuplaValue<Text, IntWritable> val: values){
                    context.write(key, val.getLeft());
                }
            }

        }

    }
}
