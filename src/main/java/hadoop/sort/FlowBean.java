package hadoop.sort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowBean implements WritableComparable<FlowBean> {

    private String phoneNum;
    private int upflow;
    private int downflow;
    private int sumflow;

    public FlowBean() {
    }

    public FlowBean(String phoneNum, int upflow, int downflow, int sumflow) {
        this.phoneNum = phoneNum;
        this.upflow = upflow;
        this.downflow = downflow;
        this.sumflow = sumflow;
    }

    public String getPhoneNum() {
        return phoneNum;
    }

    public void setPhoneNum(String phoneNum) {
        this.phoneNum = phoneNum;
    }

    public int getUpflow() {
        return upflow;
    }

    public void setUpflow(int upflow) {
        this.upflow = upflow;
    }

    public int getDownflow() {
        return downflow;
    }

    public void setDownflow(int downflow) {
        this.downflow = downflow;
    }

    public int getSumflow() {
        return sumflow;
    }

    public void setSumflow(int sumflow) {
        this.sumflow = sumflow;
    }

    @Override
    public String toString() {
        return phoneNum + '\t' +upflow + "\t" + downflow + "\t" + sumflow;
    }

    //map----reducer 的比较规则  按照总流量进行排序：倒序
    public int compareTo(FlowBean o) {
        return o.getSumflow()-this.getSumflow();
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(phoneNum);
        out.writeInt(upflow);
        out.writeInt(downflow);
        out.writeInt(sumflow);
    }

    public void readFields(DataInput in) throws IOException {

        this.phoneNum=in.readUTF();
        this.upflow=in.readInt();
        this.downflow=in.readInt();
        this.sumflow=in.readInt();
    }
}
