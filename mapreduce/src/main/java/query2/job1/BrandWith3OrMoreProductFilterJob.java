package query2.job1;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import utils.TextIntTuplaValue;
import utils.TripleValue;
import utils.TuplaValue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class BrandWith3OrMoreProductFilterJob {

    /**
     * Mapper for job1
     */
    public static class Brand3ProductFilterMapper extends Mapper<
            LongWritable, Text,
            Text, TextIntTuplaValue> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            //INPUT: Leggo file
            final String[] metaAttributes = value.toString().split(",", -1);
            final String brand = metaAttributes[0].trim() + ",";
            final String prodID = metaAttributes[1].trim();

            //OUTPUT: ((brand), prodID, 1)
            context.write(new Text(brand), new TextIntTuplaValue(new Text(prodID), new IntWritable(1)));

        }

    }

    /**
     * Reducer
     */
    public static class Brand3ProductFilterReducer extends Reducer<
            Text, TextIntTuplaValue,
            Text, Text> {

        public void reduce(Text key, Iterable<TextIntTuplaValue> values, Context context) throws IOException, InterruptedException {

            //INPUT: ((brand), prodID, 1)
            int brandProductCounter = 0;
            List<Text> cache = new ArrayList<Text>();

            for(TextIntTuplaValue val: values){
                cache.add(val.getLeft());
                brandProductCounter += val.getRight().get();
            }

            //OUTPUT: ((brand), prodID)
            if(brandProductCounter >= 2){
                for(Text val: cache){
                    System.out.println("K: " + key.toString() + ", " +val.toString());
                    context.write(key, val);
                }
            }

        }

    }
}
