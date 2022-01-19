package utils.tuplaValue;

import org.apache.hadoop.io.IntWritable;

public class IntIntTuplaValue extends TuplaValue<IntWritable, IntWritable> {

    public IntIntTuplaValue() {
        super(new IntWritable(), new IntWritable());
    }

    public IntIntTuplaValue(IntWritable left, IntWritable right) {
        super(left, right);
    }
}
