package hadoop.score;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 自定义分组  需要继承一个类WritableComparator
 */
public class MyGroup extends WritableComparator {

    public MyGroup(){
        //默认情况下第二个参数为false，不会构建实例对象，所以我们需要手动调用一下，传入true
        super(ScoreBean.class,true);
    }

    //用于比较实现了WritableComparable的类的实例
    @Override
    public int compare(WritableComparable a, WritableComparable b) {

        ScoreBean asb=(ScoreBean)a;
        ScoreBean bsb=(ScoreBean)b;

        //只关心返回0的值
        return asb.getCourse().compareTo(bsb.getCourse());
    }
}
