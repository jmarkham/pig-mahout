package com.hortonworks.pig.mahout.recommend;

import java.io.IOException;

import org.apache.pig.pigunit.PigTest;
import org.apache.pig.tools.parameters.ParseException;
import org.junit.Test;

public class RecommendTest {

	@Test
	public void testSingeUserRecommendation() throws IOException,
			ParseException {

		String[] params = { "user_id=6040",
				"udf_jar=target/pig.udf.mahout-0.0.1-SNAPSHOT.jar",
				"users_file=src/test/resources/users.dat",
				"ratings_file=src/test/resources/ratings.dat"};
		PigTest test = new PigTest(
				"src/test/pig/single-user-recommendation.pig",
				params);

		test.unoverride("STORE");
		test.runScript();

		// String[] output = { "" };
		// test.assertOutput("recommendations", output);
	}
}
