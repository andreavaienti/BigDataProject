package utils;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Text;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TripleValue implements WritableComparable<TripleValue> {

    private Text left;
    private Text center = null;
    private Text right;

    public TripleValue(Text left, Text right) {
        this.left = left;
        this.right = right;
    }

    public TripleValue(Text left, Text center, Text right) {
        this.left = left;
        this.center = center;
        this.right = right;
    }

    public Boolean isTriple(){
        return center != null;
    }

    public Text getLeft() {
        return this.left;
    }

    public Text getCenter() {
        return this.center;
    }

    public Text getRight() {
        return this.right;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.left.readFields(in);
        this.center.readFields(in);
        this.right.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.left.write(out);
        this.center.write(out);
        this.right.write(out);
    }

    @Override
    public int compareTo(TripleValue o) {
        if(this.left.compareTo(o.getLeft() == 0){
            if(this.center.compareTo(o.getCenter()) == 0){
                return this.right.compareTo(o.getRight());
            }
            return this.center.compareTo(o.getCenter());
        }
        return this.left.compareTo(o.getLeft());
    }

}