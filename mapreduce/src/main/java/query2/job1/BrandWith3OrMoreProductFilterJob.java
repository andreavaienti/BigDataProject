package query2.job1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import utils.parser.MetaRecordParser;
import utils.tuplaValue.TextIntTuplaValue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * This MapReduce counts how many products each brand has.
 * Then it performs a filter that only keeps the elements of the brands with at least 2 products.
 */
public class BrandWith3OrMoreProductFilterJob {

    /**
     * Mapper for job1
     */
    public static class Brand3ProductFilterMapper extends Mapper<
            LongWritable, Text,
            Text, TextIntTuplaValue> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            if(MetaRecordParser.areParsable(value.toString())) {
                //File Format: brand, prodID
                final String[] metaAttributes = value.toString().split(",", -1);
                final String brand = metaAttributes[0].trim() + ",";
                final String prodID = metaAttributes[1].trim();

                //OUTPUT: ((brand), prodID, 1)
                context.write(new Text(brand), new TextIntTuplaValue(new Text(prodID), new IntWritable(1)));
            }
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
            List<String> cache = new ArrayList<String>();

            for(TextIntTuplaValue val: values){
                System.out.println(val.toString());
                cache.add(val.getLeft().toString());
                brandProductCounter += val.getRight().get();
            }

            System.out.println("NUMERO DI ELEMENTI PER QUESTO BRAND: " + brandProductCounter);
            System.out.println("PRODOTTI");
            System.out.println(Arrays.toString(cache.toArray()));

            //OUTPUT: ((brand), prodID)
            if(brandProductCounter >= 2){
                for(String val: cache){
                    System.out.println("K: " + key.toString() + ", " +val);
                    context.write(key, new Text(val));
                }
            }
        }
    }
}
