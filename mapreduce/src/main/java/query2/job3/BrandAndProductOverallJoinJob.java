package query2.job3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import utils.TextTextTuplaValue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class BrandAndProductOverallJoinJob {

    /**
     * Mapper for Brand With 3 Or More Products
     */
    public static class BrandMapper extends Mapper<
            LongWritable, Text,
            Text, TextTextTuplaValue> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            //INPUT: (brand, prodID)
            final String[] metaAttributes = value.toString().split(",", -1);
            final String brand = metaAttributes[0].trim();
            final String prodID = metaAttributes[1].trim();

            //OUTPUT: ((prodID), (source, brand))
            context.write(new Text(prodID), new TextTextTuplaValue(new Text("brand"), new Text(brand)));

        }

    }

    /**
     * Mapper for Product Overall
     */
    public static class ProductOverallMapper extends Mapper<
            LongWritable, Text,
            Text, TextTextTuplaValue>{


        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            //INPUT: ((prodID), overall)
            final String[] productOverallAttributes = value.toString().split(",", -1);
            final String prodID = productOverallAttributes[0].trim();
            final String overall = productOverallAttributes[1].trim();

            //OUTPUT: ((prodID), (source, overall))
            context.write(new Text(prodID), new TextTextTuplaValue(new Text("overall"), new Text(overall)));

        }

    }

    /**
     * Reducer
     */
    public static class JoinReducer extends Reducer<
            Text, TextTextTuplaValue,
            Text, TextTextTuplaValue> {

        public void reduce(Text key, Iterable<TextTextTuplaValue> values, Context context) throws IOException, InterruptedException {

            String brand = "";
            String overall = "0.0";
            List<String> joinRecords = new ArrayList<String>();

            System.out.println("JOIN REDUCEEEEEEEEEEEEEEEEEEEEEEEEEER");

            for(TextTextTuplaValue val : values) {
                System.out.println("K:" + key.toString() + " V:" + val.toString());
                if(val.getLeft().toString().equals("brand")) {
                    brand = val.getRight().toString();
                } else {
                    //if(!val.getRight().toString().isEmpty())
                    overall = val.getRight().toString();
                }
                System.out.println(brand);
                System.out.println(overall);
            }
            System.out.println("TOTALE");
            System.out.println(brand);
            System.out.println(overall);

            //OUTPUT: (prodID, (brand, overall))
            if(!overall.equals("0.0"))
                context.write(new Text(key.toString() + ","), new TextTextTuplaValue(new Text(brand), new Text(overall)));

        }

    }

}
