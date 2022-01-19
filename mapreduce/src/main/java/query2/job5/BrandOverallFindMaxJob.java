package query2.job5;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import utils.TextDoubleTuplaValue;
import utils.TuplaValue;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class BrandOverallFindMaxJob {

    /**
     * Mapper for job4
     */
    public static class BrandOverallMaxMapper extends Mapper<
            LongWritable, Text,
            IntWritable, TextDoubleTuplaValue> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            final String[] brandOverallAttributes = value.toString().split(",", -1);
            final String brand = brandOverallAttributes[0].trim();
            final String overall = brandOverallAttributes[1].trim();
            //INPUT: (brand, overall))
            //OUTPUT: (1, (brand, overall))
            context.write(new IntWritable(1), new TextDoubleTuplaValue(new Text(brand), new DoubleWritable(Double.parseDouble(overall))));

        }
    }

    /**
     * Combiner
     */
    public static class BrandOverallMaxCombiner extends Reducer<
            IntWritable, TextDoubleTuplaValue,
            IntWritable, TextDoubleTuplaValue> {

        public void reduce(IntWritable key, Iterable<TextDoubleTuplaValue> values, Context context) throws IOException, InterruptedException {

            double maxValue = Double.MIN_VALUE;
            //Text maxBrand = new Text("");
            Set<String> maxBrands = new HashSet<String>();

            for(TextDoubleTuplaValue val: values){
                if(maxValue < val.getRight().get()){
                    maxValue = val.getRight().get();
                    maxBrands.clear();
                    maxBrands.add(val.getLeft().toString());
                }  else if(maxValue == val.getRight().get()){
                    maxBrands.add(val.getLeft().toString());
                }
            }

            for(String brand: maxBrands)
                context.write(key, new TextDoubleTuplaValue(new Text(brand), new DoubleWritable(maxValue)));

        }

    }

    /**
     * Reducer
     */
    public static class BrandOverallMaxReducer extends Reducer<
            IntWritable, TextDoubleTuplaValue,
            DoubleWritable, Text> {

        public void reduce(IntWritable key, Iterable<TextDoubleTuplaValue> values, Context context) throws IOException, InterruptedException {

            double maxValue = Double.MIN_VALUE;
            Set<String> maxBrands = new HashSet<String>();
            String maxBrandsFileFormat = "";

            System.out.println("REDUCEEEEEEEEER");

            for(TextDoubleTuplaValue val: values){
                System.out.println("Brand:" + val.getLeft().toString() + " Overall: " + val.getRight().toString());
                if(maxValue < val.getRight().get()){
                    maxValue = val.getRight().get();
                    System.out.println("PULISCO SET");
                    maxBrands.clear();
                    System.out.println("AGGIUNGO BRAND");
                    maxBrands.add(val.getLeft().toString());
                }  else if(maxValue == val.getRight().get()){
                    System.out.println("AGGIUNGO BRAND");
                    maxBrands.add(val.getLeft().toString());
                }
            }

            System.out.println("BRAND CON OVERALL MASSIMO");
            for(String brand: maxBrands) {
                System.out.println(brand);
                maxBrandsFileFormat = maxBrandsFileFormat.concat(brand + "-");
            }

            context.write(new DoubleWritable(maxValue), new Text(maxBrandsFileFormat.substring(0, maxBrandsFileFormat.length() - 1)));

        }

    }
}
