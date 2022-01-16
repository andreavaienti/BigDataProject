package query2.job5;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import utils.TuplaValue;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class BrandOverallFindMaxJob {

    /**
     * Mapper for job4
     */
    public static class BrandOverallMaxMapper extends Mapper<
            Text, DoubleWritable,
            IntWritable, TuplaValue<DoubleWritable, Text>> {

        public void map(Text key, DoubleWritable value, Context context) throws IOException, InterruptedException {

            //INPUT: (brand, overall))
            //OUTPUT: (1, (overall, brand))
            context.write(new IntWritable(1), new TuplaValue<DoubleWritable, Text>(value, key));

        }
    }

    /**
     * Combiner
     */
    public static class BrandOverallMaxCombiner extends Reducer<
            IntWritable, TuplaValue<DoubleWritable, Text>,
            IntWritable, TuplaValue<DoubleWritable, Text>> {

        public void reduce(IntWritable key, Iterable<TuplaValue<DoubleWritable, Text>> values, Context context) throws IOException, InterruptedException {

            double maxValue = Double.MIN_VALUE;
            Text maxBrand = new Text("");

            for(TuplaValue<DoubleWritable, Text> val: values){
                if(maxValue < val.getLeft().get()){
                    maxValue = val.getLeft().get();
                    maxBrand = val.getRight();
                }
            }

            context.write(key, new TuplaValue<DoubleWritable, Text>(new DoubleWritable(maxValue), maxBrand));

        }

    }

    /**
     * Reducer
     */
    public static class BrandOverallMaxReducer extends Reducer<
            IntWritable, TuplaValue<DoubleWritable, Text>,
            Text, DoubleWritable> {

        public void reduce(IntWritable key, Iterable<TuplaValue<DoubleWritable, Text>> values, Context context) throws IOException, InterruptedException {

            double maxValue = Double.MIN_VALUE;
            Set<String> maxBrands = new HashSet<String>();

            for(TuplaValue<DoubleWritable, Text> val: values){
                if(maxValue < val.getLeft().get()){
                    maxValue = val.getLeft().get();
                    maxBrands.clear();
                    maxBrands.add(val.getRight().toString());
                } else if(maxValue == val.getLeft().get()){
                    maxBrands.add(val.getRight().toString());
                }
            }

            for(String brand: maxBrands){
                context.write(new Text(brand), new DoubleWritable(maxValue));
            }

        }

    }
}
