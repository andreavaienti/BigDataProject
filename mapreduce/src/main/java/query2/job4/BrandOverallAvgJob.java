package query2.job4;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import utils.tuplaValue.DoubleDoubleTuplaValue;

import java.io.IOException;

public class BrandOverallAvgJob {

    /**
     * Mapper for job4
     */
    public static class BrandOverallAvgMapper extends Mapper<
            LongWritable, Text,
            Text, DoubleDoubleTuplaValue> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            //INPUT: (prodID, (brand, overall))
            final String[] joinAttributes = value.toString().split(",", -1);
            final String brand = joinAttributes[1].trim();
            final String overall = joinAttributes[2].trim();

            //INPUT: (prodID, (brand, overall))
            //OUTPUT: (brand, overall, 1)
            context.write(new Text(brand), new DoubleDoubleTuplaValue(new DoubleWritable(Double.parseDouble(overall)), new DoubleWritable(1)));

        }
    }

    /**
     * Combiner
     */
    public static class BrandOverallAvgCombiner extends Reducer<
            Text, DoubleDoubleTuplaValue,
            Text, DoubleDoubleTuplaValue> {

        public void reduce(Text key, Iterable<DoubleDoubleTuplaValue> values, Context context) throws IOException, InterruptedException {

            double sumLocalOverall = 0, sumLocalItem = 0;

            for(DoubleDoubleTuplaValue val: values){
                sumLocalOverall += val.getLeft().get();
                sumLocalItem += val.getRight().get();
            }

            System.out.println("RISULTATO PARZIALE");
            System.out.println("K:" + key.toString() + " V:" + sumLocalOverall/sumLocalItem);
            context.write(key, new DoubleDoubleTuplaValue(new DoubleWritable(sumLocalOverall), new DoubleWritable(sumLocalItem)));

        }

    }

    /**
     * Reducer
     */
    public static class BrandOverallAvgReducer extends Reducer<
            Text, DoubleDoubleTuplaValue,
            Text, DoubleWritable> {

        public void reduce(Text key, Iterable<DoubleDoubleTuplaValue> values, Context context) throws IOException, InterruptedException {

            double sumTotalOverall = 0, sumTotalItem = 0;

            for(DoubleDoubleTuplaValue val: values){
                sumTotalOverall += val.getLeft().get();
                sumTotalItem += val.getRight().get();
            }
            System.out.println("RISULTATO TOTALE");
            System.out.println("K:" + key.toString() + " V:" + sumTotalOverall/sumTotalItem);
            //OUTPUT: ((prodID), overall)
            context.write(new Text(key.toString() + ","),new DoubleWritable(sumTotalOverall/sumTotalItem));

        }

    }
}
