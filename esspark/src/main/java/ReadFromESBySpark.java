import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

import java.util.Map;

public class ReadFromESBySpark {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("my-app").clone()
                .set("es.nodes", "10.0.4.17")
                .set("es.port", "9200")
                .set("es.nodes.wan.only","true")
                .set("es.resource", "logs-201998/type")
                .set("es.input.use.sliced.partitions", "false")
                .set("es.input.max.docs.per.partition", "100000000")
                .set("es.query", "?q=clientip:247.37.0.0");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaPairRDD<String, Map<String, Object>> rdd = JavaEsSpark.esRDD(sc);

        for ( Map<String, Object> item : rdd.values().collect()) {
            System.out.println(item);
        }

        sc.stop();
    }
}
