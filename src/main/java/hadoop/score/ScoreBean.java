package hadoop.score;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 自定义分组和排序使用的对象
 */
public class ScoreBean implements WritableComparable<ScoreBean> {

    private String course;
    private double score;

    public ScoreBean() {
    }

    public ScoreBean(String course, double score) {
        this.course = course;
        this.score = score;
    }

    public String getCourse() {
        return course;
    }

    public void setCourse(String course) {
        this.course = course;
    }

    public double getScore() {
        return score;
    }

    public void setScore(double score) {
        this.score = score;
    }

    @Override
    public String toString() {
        return course + '\t' +score;
    }

    // 定义的排序规则，按照分数降序排列
    public int compareTo(ScoreBean o) {
        //排序还按照思路实现可以不？如果改进怎么改进，先按照课程排序，相同的课程肯定在一起，再按照分数排序
        //先进行课程比较 目的是到reduce端相同的课程会在一起 排在一起
        int tmp=this.getCourse().compareTo(o.getCourse());
        if(tmp==0){
            //课程相同的时候再进行排序
            return o.getScore()-this.getScore()>0?1:(o.getScore()-this.getScore()==0?0:-1);
        }
        return tmp;
    }

    public void write(DataOutput out) throws IOException {

        out.writeUTF(this.course);
        out.writeDouble(this.score);
    }

    public void readFields(DataInput in) throws IOException {

        this.course=in.readUTF();
        this.score=in.readDouble();
    }
}
