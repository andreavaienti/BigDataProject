package utils;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

public class TextDoubleTuplaValue extends TuplaValue<Text, DoubleWritable>{

    public TextDoubleTuplaValue() {
        super(new Text(), new DoubleWritable());
    }

    public TextDoubleTuplaValue(Text left, DoubleWritable right) {
        super(left, right);
    }
}