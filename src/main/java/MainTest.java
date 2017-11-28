import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;

import java.util.Arrays;
import java.util.List;

/**
 * @author wangyanghua
 * @create 2017/11/24
 */
public class MainTest {
  public static void main(String[] args) {
    test();
  }

  public static void test() {
    SparkConf conf = new SparkConf().setAppName("TestSparkApp").setMaster("spark://172.28.49.28:7077")
            .setJars(new String[]{"/data0/sparkapp/jar/MainTest.jar"});
    JavaSparkContext sc = new JavaSparkContext(conf);
    List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
//    JavaRDD<Integer> distData = sc.parallelize(data);
//    int totalLength = distData.reduce((a, b) -> a + b);
    int totalLength = sc.parallelize(data).reduce((a, b) -> a + b);
    System.out.println("Counter value: " + totalLength);
  }
}