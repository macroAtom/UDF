//所在的包名，包名是唯一的
package org.xxl.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
 * 计算字符串的长度
 */
public class MyUdfLength extends GenericUDF {
    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {

        if (objectInspectors.length !=1){
            throw new UDFArgumentException("参数个数不为1");
        }

        ObjectInspector outputOI = null;
        // 返回的类型
        outputOI = PrimitiveObjectInspectorFactory.writableIntObjectInspector;
        return outputOI;
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        //1.取出输入数据
        String input = deferredObjects[0].get().toString();

        //2.判断输入数据是否为空
        if (input == null){
            return 0;
        }

        return input.length();
//        return 100;
    }

    @Override
    public String getDisplayString(String[] strings) {
        return null;
    }
}
