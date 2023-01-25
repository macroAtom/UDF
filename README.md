# 如何使用
# 将生成的jar包拷贝到hive所在的lib库，例如：/opt/hive/lib/
# 进入hive客户端，如果在hive客户端可以使用add jar 或退出重新进入hive客户端
# 可以通过如下示例验证结果，如果没有问题，第一个应该是5，第二个会生成新的列 word：a,b
create temporary function myudf_len101 as "org.xxl.udf.MyUdfLength";
select myudf_len101("abcde") from student;
create temporary function myudtf_split102 as "org.xxl.udf.myUdtfExplode";
select myudtf_split102("a,b",',') from student;
