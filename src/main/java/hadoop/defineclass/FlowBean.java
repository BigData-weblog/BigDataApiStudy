package hadoop.defineclass;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


/**
 * 自定义类
 */
public class FlowBean implements Writable {
    private int upflow;
    private int downflow;
    private int sumflow;

    public FlowBean() {
    }

    public FlowBean(int upflow, int downflow) {
        this.upflow = upflow;
        this.downflow = downflow;
        this.sumflow=this.upflow+this.downflow;
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
        return upflow + "\t" + downflow + "\t" + sumflow;
    }

    //序列化的方法
    public void write(DataOutput dataOutput) throws IOException {

        //每一个属性都要进行序列化
        dataOutput.writeInt(upflow);
        dataOutput.writeInt(downflow);
        dataOutput.writeInt(sumflow);
    }
    //反序列化 注意序列化和反序列化的对应关系
    public void readFields(DataInput dataInput) throws IOException {

        this.upflow=dataInput.readInt();
        this.downflow=dataInput.readInt();
        this.sumflow=dataInput.readInt();

    }
}
