package org.xxl.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.List;

public class myUdtfExplode extends GenericUDTF {

    // 输出数据的集合
    private ArrayList<String> outPutList = new ArrayList<>();
    
    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {

        // 比如：hive,spark,flink
        // 分裂后，假设列名为word
        // hive
        // spark
        // flink
        // 分裂后，生成的新的列名
        List<String> fieldNames = new ArrayList<>();
        List<ObjectInspector> fieldOIs = new ArrayList<>();

        // 新生成的列的列名
        fieldNames.add("word");
        // 新生成的列的类型
        fieldOIs.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public void process(Object[] objects) throws HiveException {
        // 接收要处理的数据，处理的数据是以逗号分割的字符串；例如：hello,a,hive,spark 等等
        /**
         * 输出数据：
         * hello
         * a
         * hive
         * spark
         */

        // 取出输入数据
        String input = objects[0].toString();
        String split = objects[1].toString();

        // 对输入数据进行分割后放到一个数组里
        String[] words = input.split(split);

        // 遍历输入的参数值，并将其添加到outPutList 中
        for(String word:words){
            // 清空输出收集器
            outPutList.clear();
            // 将分离的字符串添加到输出收集器中
            outPutList.add(word);

            // 通过forward 输出到缓存收集器中
            forward(outPutList);
        }
    }

    @Override
    public void close() throws HiveException {

    }
}
