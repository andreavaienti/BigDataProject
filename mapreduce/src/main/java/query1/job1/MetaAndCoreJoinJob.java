package query1.job1;

import org.apache.hadoop.io.IntWritable;
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
    public static class MetaMapper extends Mapper<IntWritable, Text, Text, TripleValue> {

        private Text prodIDText = new Text(), sourceText = new Text(), brandText = new Text();

        public void map(IntWritable key, Text value, Context context) throws IOException, InterruptedException {

            // Mapper logic

            // Output should be formatted as (joinKey, value), (prodID, "meta")
            // where the value also specifies which is the source. It can be either:
            // - a string formatted like "source-value" to be parsed by the reducer
            // - an object of a custom class that contains both information
            final String[] metaAttributes = value.toString().split(",", -1);
            final String brand = metaAttributes[0].trim();
            final String prodID = metaAttributes[1].trim();
            prodIDText.set(prodID);
            sourceText.set("meta");
            brandText.set(brand);

            context.write(prodIDText, new TripleValue(sourceText, brandText));

        }

    }

    /**
     * Mapper for 5-CORE dataset
     */
    public static class CoreMapper extends Mapper<IntWritable, Text, Text, TripleValue>{

        private Text prodIDText = new Text(), sourceText = new Text(), voteText = new Text(), revIDText = new Text();

        public void map(IntWritable key, Text value, Context context) throws IOException, InterruptedException {

            // Mapper logic

            // Output should be formatted as (joinKey, value), (prodID, "core")
            // where the value also specifies which is the source. It can be either:
            // - a string formatted like "source-value" to be parsed by the reducer
            // - an object of a custom class that contains both information
            final String[] coreAttributes = value.toString().split(",", -1);
            final String revID = coreAttributes[1].trim();
            final String prodID = coreAttributes[2].trim();
            final String vote = coreAttributes[4].trim();
            prodIDText.set(prodID);
            sourceText.set("core");
            voteText.set(vote);
            revIDText.set(revID);

            context.write(prodIDText, new TripleValue(sourceText, revIDText, voteText));

        }

    }

    /**
     * Reducer
     */
    public static class JobReducer extends Reducer<Text,TripleValue,Text,TripleValue> {

        private class StringTuple {

            Text revID;
            Text vote;

            StringTuple(Text revID, Text vote){
                this.revID = revID;
                this.vote = vote;
            }

            public Text getRevID() {
                return revID;
            }

            public Text getVote() {
                return vote;
            }
        }

        public void reduce(Text key, Iterable<TripleValue> values, Context context) throws IOException, InterruptedException {

            Text brand = null;
            List<StringTuple> coreDatasetRecords = new ArrayList<StringTuple>(); //(revID, vote)

            //(prodID, (source, brand) SOLO UN'OCCORRENZA
            //(prodID, (source, revID, vote) PIU DI UNA
            for(TripleValue val : values) {
                if(val.getLeft().toString() == "core")
                    coreDatasetRecords.add(new StringTuple(val.getCenter(), val.getRight()));
                else
                    brand = val.getRight();
            }

            //OUTPUT: (prodID, (brand, revID, vote)
            for(StringTuple coreItem : coreDatasetRecords) {
                context.write(key, new TripleValue(brand, coreItem.revID, coreItem.vote));
            }
        }

    }
}
