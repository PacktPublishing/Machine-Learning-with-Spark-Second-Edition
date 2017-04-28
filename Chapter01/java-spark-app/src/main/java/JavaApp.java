import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import java.util.*;
import java.util.stream.Collectors;

/**
 * A simple Spark app in Java (Wordcount example)
 */
public class JavaApp {

    public static void main(String[] args) {
        JavaSparkContext sc = null;

        try {
//           ####### create spark context by starting the cluster locally using 2 CPU cores #######
            sc = new JavaSparkContext("local[2]", "First Spark App");

            JavaRDD<String[]> data = sc.textFile("data/UserPurchaseHistory.csv").map(s -> s.split(","));

//           ####### let's count the number of purchases #######
            long numPurchases = data.count();

            System.out.println("NumberPurchases : " + numPurchases);

            long uniqueUsers = data.map(strings -> strings[0]).distinct().count();

//           ####### let's count the number of users #######
            System.out.println("Unique users : " + uniqueUsers);

            Double totalRevenue = data.map(strings -> Double.parseDouble(strings[2])).reduce((Double v1, Double v2) -> new Double(v1.doubleValue() + v2.doubleValue()));

//           ####### let's count the total revenue #######
            System.out.println("Total revenue : " + totalRevenue);

            List<Tuple2<String, Integer>> pairs = data.mapToPair(strings -> new Tuple2<String, Integer>(strings[1], 1)).reduceByKey((Integer i1, Integer i2) -> i1 + i2).collect();

            Map<String, Integer> sortedData = new HashMap<>();
            Iterator it = pairs.iterator();
            while (it.hasNext()) {
                Tuple2<String, Integer> o = (Tuple2<String, Integer>) it.next();
                sortedData.put(o._1(), o._2());
            }
            List<String> sorted = sortedData.entrySet()
                    .stream()
                    .sorted(Comparator.comparing((Map.Entry<String, Integer> entry) -> entry.getValue()).reversed())
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toList());
            System.out.println("Most popular products sorted : " + sorted);


            String mostPopular = sorted.get(0);
            int purchases = sortedData.get(mostPopular);
            System.out.println("Most popular product is : " + mostPopular + ", with number of purchases : " + purchases);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            sc.stop();
        }


    }
}
