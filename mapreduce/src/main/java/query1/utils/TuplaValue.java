package query1.utils;

import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TuplaValue<L extends WritableComparable, R extends WritableComparable>
        implements WritableComparable<TuplaValue<L, R>> {

    private L left;
    private R right;

    public TuplaValue(L left, R right) {
        this.left = left;
        this.right = right;
    }

    public L getLeft() {
        return this.left;
    }

    public R getRight() {
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
    public int compareTo(TuplaValue<L, R> o) {
        if(this.left.compareTo(o.getLeft()) == 0)
            return this.right.compareTo(o.getRight());
        return this.left.compareTo(o.getLeft());
    }
}