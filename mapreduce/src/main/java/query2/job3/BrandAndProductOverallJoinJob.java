package query2.job3;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import query1.job1.MetaAndCoreJoinJob;
import utils.TripleValue;
import utils.TuplaValue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class BrandAndProductOverallJoinJob {

    /**
     * Mapper for Brand With 3 Or More Products
     */
    public static class BrandMapper extends Mapper<
            Text, Text,
            Text, TuplaValue<Text, Text>> {

        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {

            //INPUT: ((brand), prodID)
            //OUTPUT: ((prodID), (source, brand))
            context.write(value, new TuplaValue<Text, Text>(new Text("brand"), key));

        }

    }

    /**
     * Mapper for Product Overall
     */
    public static class ProductOverallMapper extends Mapper<
            Text, DoubleWritable,
            Text, TuplaValue<Text, Text>>{


        public void map(Text key, DoubleWritable value, Context context) throws IOException, InterruptedException {

            //INPUT: ((prodID), overall)
            //OUTPUT: ((prodID), (source, overall))
            context.write(key, new TuplaValue<Text, Text>(new Text("overall"), new Text(String.valueOf(value.get()))));

        }

    }

    /**
     * Reducer
     */
    public static class JobReducer extends Reducer<
            Text, TuplaValue<Text, Text>,
            Text, TuplaValue<Text, Text>> {

        public void reduce(Text key, Iterable<TuplaValue<Text, Text>> values, Context context) throws IOException, InterruptedException {

            Text brand = null;
            Text overall = null;

            //(prodID, (source, brand) SOLO UN'OCCORRENZA
            //(prodID, (source, overall) SOLO UNA
            for(TuplaValue<Text, Text> val : values) {
                if(val.getLeft().toString() == "brand")
                    brand = val.getRight();
                else
                    overall = val.getRight();
            }

            //OUTPUT: (prodID, (brand, overall))
            context.write(key, new TuplaValue<Text, Text>(brand, overall));
        }

    }

}
