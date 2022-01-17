package utils;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Text;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TripleValue implements WritableComparable<TripleValue> {

    private Text left;
    private Text center;
    private Text right;

    public TripleValue(){
        this.left = new Text();
        this.center = new Text();
        this.right = new Text();
    }

    public TripleValue(Text left, Text right) {
        this.left = left;
        this.center = new Text();
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
        /*this.left = new Text(in.readLine());
        this.center = new Text(in.readLine());
        this.right = new Text(in.readLine());*/
        this.left.readFields(in);
        this.right.readFields(in);
        this.center.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        /*out.writeBytes(left.toString());
        if(center != null){
            out.writeBytes(center.toString());
        }
        out.writeBytes(right.toString());*/
        this.left.write(out);
        this.right.write(out);
        this.center.write(out);

    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof TripleValue) {
            TripleValue other = (TripleValue) obj;
            return left.equals(other.left) && center.equals(other.center) && right.equals(other.right);
        }
        return false;
    }

    @Override
    public int compareTo(TripleValue o) {
        if(this.left.compareTo(o.getLeft()) == 0){
            if(this.center.compareTo(o.getCenter()) == 0){
                return this.right.compareTo(o.getRight());
            }
            return this.center.compareTo(o.getCenter());
        }
        return this.left.compareTo(o.getLeft());
    }

    @Override
    public String toString() {
        return "," + left + "," + center + "," + right;
    }
}