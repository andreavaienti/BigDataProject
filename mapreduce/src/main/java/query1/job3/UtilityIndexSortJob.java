package query1.job3;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import query1.utils.TuplaValue;

import java.io.IOException;

public class UtilityIndexSortJob {

    /**
     * Mapper for job3
     */
    public static class UtilityIndexSortMapper extends Mapper<
            TuplaValue<Text, Text>, DoubleWritable,
            TuplaValue<Text, DoubleWritable>, Text> {

        public void map(TuplaValue<Text, Text> key, DoubleWritable value, Context context) throws IOException, InterruptedException {

            //OUTPUT: ((brand, utilityIndex), vote)
            context.write(new TuplaValue<Text, DoubleWritable>(key.getLeft(), value), key.getRight());

        }
    }

    /**
     * Reducer
     */
    public static class UtilityIndexSortReducer extends Reducer<
            TuplaValue<Text, DoubleWritable>, Text,
            TuplaValue<Text, DoubleWritable>, Iterable<Text>> {

        public void reduce(TuplaValue<Text, DoubleWritable> key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            //OUTPUT: ((brand, utilityIndex), (revID1, revID57, ...))
            context.write(key, values);

        }

    }
}
