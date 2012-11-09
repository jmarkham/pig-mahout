package com.hortonworks.pig.mahout.recommend;

import java.io.IOException;

import org.apache.pig.pigunit.PigTest;
import org.apache.pig.tools.parameters.ParseException;
import org.junit.Test;

public class RecommendTest {

	@Test
	public void testSingleUserRecommendation() throws IOException,
			ParseException {

		String[] params = { "user_id=6040",
				"udf_jar=target/pig.udf.mahout-0.0.1-SNAPSHOT.jar",
				"ratings_file=src/test/resources/ratings.dat" };
		PigTest test = new PigTest(
				"src/test/pig/single-user-recommendation.pig", params);

		//test.unoverride("STORE");

		String[] output = { "({(6040,{(557,5),(134,5),(3888,4),(53,4),(2480,4),(2309,4),(3245,4),(3338,4),(2931,4),(787,4)})})" };
		test.assertOutput("recommendations", output);
	}
}
