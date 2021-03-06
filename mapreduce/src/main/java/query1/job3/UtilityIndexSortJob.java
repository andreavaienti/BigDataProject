package query1.job3;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import utils.tuplaValue.TextDoubleTuplaValue;

import java.io.IOException;

/**
 * Sort for each brand the users with the highest utility index (in ascending order)
 */
public class UtilityIndexSortJob {

    /**
     * Mapper for job3
     */
    public static class UtilityIndexSortMapper extends Mapper<
            LongWritable, Text,
            TextDoubleTuplaValue, Text> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            //INPUT: ((brand, revID), utilityIndex)
            final String[] metaAttributes = value.toString().split(",", -1);
            final String brand = metaAttributes[0].trim();
            final String revID = metaAttributes[1].trim();
            final String utilityIndex = metaAttributes[2].trim();

            //OUTPUT: ((brand, utilityIndex), revID)
            context.write(new TextDoubleTuplaValue(new Text(brand), new DoubleWritable(Double.parseDouble(utilityIndex))), new Text(revID));

        }
    }

    /**
     * Reducer
     */
    public static class UtilityIndexSortReducer extends Reducer<
            TextDoubleTuplaValue, Text,
            TextDoubleTuplaValue, Text> {

        public void reduce(TextDoubleTuplaValue key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            //INPUT: ((brand, utilityIndex), revID)
            String revIDs = "";

            for(Text val: values){
                revIDs = revIDs.concat(val.toString() + ",");
            }

            //OUTPUT: ((brand, utilityIndex), (revID1, revID57, ...))
            context.write(key, new Text(revIDs.substring(0, revIDs.length() - 1)));

        }

    }
}
