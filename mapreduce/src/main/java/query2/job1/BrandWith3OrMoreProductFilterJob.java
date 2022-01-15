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
            Text, Text> {

        public void map(IntWritable key, Text value, Context context) throws IOException, InterruptedException {

            final String[] metaAttributes = value.toString().split(",", -1);
            final String brand = metaAttributes[0].trim();
            final String prodID = metaAttributes[1].trim();

            //INPUT: Leggo file
            //OUTPUT: ((brand), prodID)
            context.write(new Text(brand), new Text(prodID));

        }

    }

    /**
     * Reducer
     */
    public static class UtilityIndexSortReducer extends Reducer<
            Text, Text,
            Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {


            //
            //values.

        }

    }
}
