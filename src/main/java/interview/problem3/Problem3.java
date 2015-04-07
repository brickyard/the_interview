package interview.problem3;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/*
	Given the specified dataset of property records, provide a report that returns list of policies that have at least 10 properties nearby (within 100 miles).  
	The report should be in csv format and sorted by the count in descending order.
	
	Format Example:
	policyID,county,nearby_policy_count
	545488,HILLSBOROUGH COUNTY,25
	470914,PINELLAS COUNTY,12
	
	NOTES:
	* SOLUTION MUST UTILIZE APACHE SPARK FOR PROCESSING
	* USE OF ADDITIONAL OPEN-SOURCE LIBRARIES IS ALLOWED
	* COPYING OF CODE FROM OTHER WEB-SITES MUST BE DECLARED IN COMMENTS
	* UNIT TESTS ARE STRONGLY ENCOURAGED
	* USE 'data/problem3' FOR TESTING
 */
public class Problem3 {

	public static final String INPUT_FILE = "data/problem3/FL_insurance_sample.csv";

	public static void main(String[] args) {
		JavaSparkContext sc = new JavaSparkContext("local", Problem3.class.getName());

		JavaRDD<String> inData = sc.textFile(INPUT_FILE);

		inData.foreach(e -> System.out.println(e));

		sc.stop();
	}
}
