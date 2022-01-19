package utils.tuplaValue;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class TextIntTuplaValue extends TuplaValue<Text, IntWritable> {

    public TextIntTuplaValue() {
        super(new Text(), new IntWritable());
    }

    public TextIntTuplaValue(Text left, IntWritable right) {
        super(left, right);
    }
}
