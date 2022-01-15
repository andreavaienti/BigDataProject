package query1.utils;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TuplaValue implements WritableComparable<TuplaValue> {

    private Text left;
    private Text right;

    public TuplaValue(Text left, Text right) {
        this.left = left;
        this.right = right;
    }

    public Text getLeft() {
        return this.left;
    }

    public Text getRight() {
        return this.right;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.left.readFields(in);
        this.right.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.left.write(out);
        this.right.write(out);
    }

    @Override
    public int compareTo(TuplaValue o) {
        return this.left.compareTo(o.getLeft());
    }
}
