# spark读写es
## 打包
```
# 不执行测试用例，也不编译测试用例类。
mvn clean package -Dmaven.test.skip=true
```
```
# 不执行测试用例，但编译测试用例类生成相应的class文件至target/test-classes下。
mvn clean package -DskipTests
```
## SparkSqlWriteAndReadES
调用spark sql读写

## SparkWriteAndReadES
用spark直接读写