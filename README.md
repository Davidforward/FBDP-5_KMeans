# KMeans聚类分析算法

1. 编译打包

   使用Intelij Idea + Maven 将源代码编译打包成KMeans.jar,并将KMeans.jar传给docker容器（或虚拟机）

   

2. 准备输入

   输入文件：NewInstance.txt

   ```shell
   bin/hdfs dfs -mkdir input4
   bin/hdfs dfs -put NewInstance.txt input4
   ```

   

3. 运行（伪分布式）

   ```java
   bin/hadoop jar KMeans.jar KMeansDriver <k> <iteration num> <input> <output>
   ```

   其中：

   - <k>代表指定的簇中心数
   - <iteration num>代表迭代数
   - <input>代表输入路径
   - <output>代表输出路径

   例如：

   ```java
   bin/hadoop jar KMeans.jar KMeansDriver 5 10 input4 output4_8
   ```

   

4. 参考资料

   - 黄宜华,苗凯翔.《深入理解大数据》[M].北京：机械工程出版社，2014：295-300.
   -  [https://www.polarxiong.com/archives/Hadoop-Intellij%E7%BB%93%E5%90%88Maven%E6%9C%AC%E5%9C%B0%E8%BF%90%E8%A1%8C%E5%92%8C%E8%B0%83%E8%AF%95MapReduce%E7%A8%8B%E5%BA%8F-%E6%97%A0%E9%9C%80%E6%90%AD%E8%BD%BDHadoop%E5%92%8CHDFS%E7%8E%AF%E5%A2%83.html](https://www.polarxiong.com/archives/Hadoop-Intellij结合Maven本地运行和调试MapReduce程序-无需搭载Hadoop和HDFS环境.html) 
   -  https://hadoop.apache.org/docs/stable/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html 

   

5. 说明

   - src文件夹包含源码
   - 结果可视化文件夹包含将聚类结果可视化后形成的图片，这些图片也可以在截图.docx中看到
   - see.py是将原数据（未经KMeans聚类分析处理）画成散点图的python代码
   - seeOutcome.py是将经KMeans聚类分析处理后的结果画成散点图的python代码
   - 截图.docx不仅包括KMeans聚类分析的可视化结果，也包括一些运行的截图
   - NewInstance.txt是所使用的数据