package utils.tuplaValue;

import org.apache.hadoop.io.Text;

public class TextTextTuplaValue extends TuplaValue<Text, Text> {

    public TextTextTuplaValue() {
        super(new Text(), new Text());
    }

    public TextTextTuplaValue(Text left, Text right) {
        super(left, right);
    }
}
