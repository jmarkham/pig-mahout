package com.hortonworks.pig.mahout.recommend;

import java.io.IOException;
import java.util.List;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.pig.Accumulator;
import org.apache.pig.Algebraic;
import org.apache.pig.EvalFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;

public class Recommendation extends EvalFunc<DataBag> implements
		Accumulator<DataBag> {

	private BagFactory bagFactory = BagFactory.getInstance();
	private TupleFactory tupleFactory = TupleFactory.getInstance();
	private DataBag intermediateRecommendation = null;
	private long userID;
	private int howMany = 10;

	public Recommendation() {

	}

	public Recommendation(String userID, String howMany) {
		this.userID = Long.parseLong(userID);
		this.howMany = Integer.parseInt(howMany);
	}

	@Override
	public DataBag exec(Tuple record) throws IOException {
		DataBag bag = bagFactory.newDefaultBag();
		bag.add(record);
		try {
			return recommend(bag);
		} catch (TasteException e) {
			String msg = "Error while computing recommendations in "
					+ this.getClass().getSimpleName();
			throw new ExecException(msg, PigException.ERROR, e);
		}
	}

	public void accumulate(Tuple records) throws IOException {
		try {
			DataBag bag = (DataBag) records.get(0);
			intermediateRecommendation = recommend(bag);
		} catch (Exception e) {
			String msg = "Error while computing recommendations in "
					+ this.getClass().getSimpleName();
			throw new ExecException(msg, PigException.ERROR, e);
		}
	}

	private DataBag recommend(DataBag bag) throws ExecException, TasteException {
		DataModel model = new BagDataModel(bag);
		GenericRecommender recommender = new GenericRecommender(model);
		List<RecommendedItem> recommendations = recommender.recommend(userID,
				howMany);

		DataBag recommendationsBag = bagFactory.newDefaultBag();
		for (RecommendedItem recommendation : recommendations) {
			Tuple tuple = tupleFactory.newTuple(2);
			tuple.set(0, recommendation.getItemID());
			tuple.set(1, recommendation.getValue());
		}
		return recommendationsBag;
	}

	public Schema outputSchema(Schema input) {
		Schema tupleSchema = new Schema();
		tupleSchema.add(new Schema.FieldSchema(null, DataType.LONG));
		tupleSchema.add(new Schema.FieldSchema(null, DataType.FLOAT));
		return tupleSchema;
	}

	public void cleanup() {
		intermediateRecommendation = null;
	}

	public DataBag getValue() {
		return intermediateRecommendation;
	}
}