package query1.job1;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import utils.TripleValue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * MapReduce job to join Meta.csv and 5-Core.csv.
 */
public class MetaAndCoreJoinJob {

    /**
     * Mapper for META dataset
     */
    public static class MetaMapper extends Mapper<
            LongWritable, Text,
            Text, TripleValue> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            // Mapper logic
            // Output should be formatted as (joinKey, value), (prodID, "meta")
            // where the value also specifies which is the source. It can be either:
            // - a string formatted like "source-value" to be parsed by the reducer
            // - an object of a custom class that contains both information

            //File Format: brand, prodID
            final String[] metaAttributes = value.toString().split(",", -1);
            final String brand = metaAttributes[0].trim();
            final String prodID = metaAttributes[1].trim();

            //OUTPUT: (prodID, ("meta", brand))
            context.write(new Text(prodID), new TripleValue(new Text("meta"), new Text(brand)));

        }

    }

    /**
     * Mapper for 5-CORE dataset
     */
    public static class CoreMapper extends Mapper<
            LongWritable, Text,
            Text, TripleValue>{

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            //File Format: overall, revID, prodID, revName, vote
            final String[] coreAttributes = value.toString().split(",", -1);
            final String revID = coreAttributes[1].trim();
            final String prodID = coreAttributes[2].trim();
            final String vote = coreAttributes[4].trim();

            //OUTPUT: (prodID, ("core", revID, vote))
            context.write(new Text(prodID), new TripleValue(new Text("core"), new Text(revID), new Text(vote)));

        }

    }

    /**
     * Reducer
     */
    public static class JobReducer extends Reducer<
            Text,TripleValue,
            Text,TripleValue> {

        public void reduce(Text key, Iterable<TripleValue> values, Context context) throws IOException, InterruptedException {

            String brand = "";
            List<String> coreDatasetRecords = new ArrayList<String>();

            //INPUT:
            //(prodID, (source, brand) SOLO UN'OCCORRENZA
            //(prodID, (source, revID, vote) PIU DI UNA
            for(TripleValue val : values) {
                if(val.getLeft().toString().equals("core")) {
                    coreDatasetRecords.add(val.getCenter().toString() + "," + val.getRight().toString());
                } else {
                    brand = val.getRight().toString();
                }
            }

            //OUTPUT: (prodID, (brand, revID, vote)
            for(String coreItem : coreDatasetRecords) {
                String[] s= coreItem.split(",");
                context.write(key, new TripleValue(new Text(brand), new Text(s[0]), new Text(s[1])));
            }
        }

    }
}
