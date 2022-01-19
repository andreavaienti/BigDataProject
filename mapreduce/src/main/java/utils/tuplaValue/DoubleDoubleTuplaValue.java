package utils.tuplaValue;

import org.apache.hadoop.io.DoubleWritable;

public class DoubleDoubleTuplaValue extends TuplaValue<DoubleWritable, DoubleWritable> {

    public DoubleDoubleTuplaValue() {
        super(new DoubleWritable(), new DoubleWritable());
    }

    public DoubleDoubleTuplaValue(DoubleWritable left, DoubleWritable right) {
        super(left, right);
    }
}