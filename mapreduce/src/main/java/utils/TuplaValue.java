package utils;

import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

public class TuplaValue<L extends WritableComparable, R extends WritableComparable>
        implements WritableComparable<TuplaValue<L, R>> {

    private L left;
    private R right;

    /*static <O, T> boolean isType(O o, T t){
        return o instanceof T;
    }*/

    private boolean isTypeInt(L t){
        return t instanceof Text;
    }

    public TuplaValue() {
        //this.left = (L) new Writable();


        this.left = new Writable();
        this.right = (R) new Object();
    }

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
    public boolean equals(Object obj) {
        if (obj instanceof TuplaValue) {
            TuplaValue<L, R> other = (TuplaValue<L, R>) obj;
            return left.equals(other.left) && right.equals(other.right);
        }
        return false;
    }

    @Override
    public int compareTo(TuplaValue<L, R> o) {
        if(this.left.compareTo(o.getLeft()) == 0)
            return this.right.compareTo(o.getRight());
        return this.left.compareTo(o.getLeft());
    }

    @Override
    public String toString() {
        return "TuplaValue{" +
                "left=" + left +
                ", right=" + right +
                '}';
    }
}