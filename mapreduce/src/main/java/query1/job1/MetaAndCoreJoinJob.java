package query1.job1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import utils.parser.CoreRecordParser;
import utils.parser.MetaRecordParser;
import utils.tripleValue.TripleValue;

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
            // Output has been formatted as (prodID, (source, brand))
            // where prodID is the sharedKey and the value first item is a string that specifies which is the source.
            if(MetaRecordParser.areParsable(value.toString())){
                //File Format: brand, prodID
                final String[] metaAttributes = value.toString().split(",", -1);
                final String brand = metaAttributes[0].trim();
                final String prodID = metaAttributes[1].trim();

                //OUTPUT: (prodID, ("meta", brand))
                //In this case, the class TripleValue is used without specifying the middle element.
                //This will allow us to have the same class as input in the reducer.
                context.write(new Text(prodID), new TripleValue(new Text("meta"), new Text(brand)));
            }
        }

    }

    /**
     * Mapper for 5-CORE dataset
     */
    public static class CoreMapper extends Mapper<
            LongWritable, Text,
            Text, TripleValue>{

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            // Mapper logic
            // Output has been formatted as (prodID, (source, reviewerID, vote))
            // where prodID is the sharedKey and the value first item is a string that specifies which is the source.

            if(CoreRecordParser.areParsable(value.toString())){
                //File Format: overall, revID, prodID, revName, vote
                final String[] coreAttributes = value.toString().split(",", -1);
                final String revID = coreAttributes[2].trim();
                final String prodID = coreAttributes[3].trim();
                final String vote = coreAttributes[1].trim();

                //OUTPUT: (prodID, ("core", revID, vote))
                context.write(new Text(prodID), new TripleValue(new Text("core"), new Text(revID), new Text(vote)));
            } else {
                System.out.println("ELEMENTO DEL CORE NON PARSABILE");
                System.out.println(value.toString());
            }


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
            //(prodID, (source, brand) -> only one record for the same prodID
            //(prodID, (source, revID, vote) -> more record for the same ProdID
            //Scorrendo tutti i valori associati a questa chiave abbiamo memorizzato il nome del brand e tutte le recensioni effettuate.
            for(TripleValue val : values) {
                if(val.getLeft().toString().equals("core")) {
                    coreDatasetRecords.add(val.getCenter().toString() + "," + val.getRight().toString());
                } else {
                    brand = val.getRight().toString();
                }
            }

            //OUTPUT: (prodID, (brand, revID, vote)
            //In questo loop avviene il vero e proprio join, in cui ad ogni prodotto vengono aggiunte le informazioni derivanti dai due dataset.
            for(String coreItem : coreDatasetRecords) {
                String[] s= coreItem.split(",");
                context.write(key, new TripleValue(new Text(brand), new Text(s[0]), new Text(s[1])));
            }
        }

    }
}
