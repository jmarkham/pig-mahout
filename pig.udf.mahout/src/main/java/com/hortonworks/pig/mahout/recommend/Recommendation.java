package com.hortonworks.pig.mahout.recommend;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
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

public class Recommendation extends EvalFunc<DataBag> implements Algebraic {

	private static BagFactory bagFactory = BagFactory.getInstance();
	private static TupleFactory tupleFactory = TupleFactory.getInstance();
	private static long userID = 6040;
	private static int howMany = 10;

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

	public String getInitial() {
		return Initial.class.getName();
	}

	public String getIntermed() {
		return Intermediate.class.getName();
	}

	public String getFinal() {
		return Final.class.getName();
	}

	static public class Initial extends EvalFunc<Tuple> {

		@Override
		public Tuple exec(Tuple input) throws IOException {
			DataBag bag = (DataBag) input.get(0);
			for (Iterator<Tuple> i = bag.iterator(); i.hasNext();) {
				return i.next();
			}
			return null;
		}
	}

	static public class Intermediate extends EvalFunc<Tuple> {

		@Override
		public Tuple exec(Tuple input) throws IOException {
			DataBag bag = (DataBag) input.get(0);
			DataBag intermediateBag = bagFactory.newDefaultBag();
			for (Iterator<Tuple> i = bag.iterator(); i.hasNext();) {
				intermediateBag.add(i.next());
			}
			Tuple tuple = tupleFactory.newTuple(1);
			tuple.set(0, intermediateBag);
			return tuple;
		}
	}

	static public class Final extends EvalFunc<DataBag> {

		@Override
		public DataBag exec(Tuple input) throws IOException {
			try {
				return recommend(unwrapFinalBag(input,
						bagFactory.newDefaultBag()));
			} catch (TasteException e) {
				String msg = "Error while computing recommendations in "
						+ this.getClass().getSimpleName();
				throw new ExecException(msg, PigException.ERROR, e);
			}
		}
	}

	private static DataBag unwrapFinalBag(Tuple input, DataBag finalBag)
			throws ExecException {

		Object elem = input.get(0);
		if (elem != null && elem instanceof DataBag) {
			DataBag nestedBag = (DataBag) elem;
			for (Iterator<Tuple> i = nestedBag.iterator(); i.hasNext();) {
				Tuple nestedTuple = i.next();
				for (Object field : nestedTuple.getAll()) {
					if (field instanceof DataBag) {
						unwrapFinalBag(nestedTuple, finalBag);
					} else {
						finalBag.add(nestedTuple);
					}
				}
			}
		}

		return finalBag;
	}

	private static DataBag recommend(DataBag bag) throws ExecException,
			TasteException {
		DataModel model = new BagDataModel(bag);
		GenericRecommender recommender = new GenericRecommender(model);
		List<RecommendedItem> recommendations = recommender.recommend(userID,
				howMany);

		DataBag recommendationsBag = bagFactory.newDefaultBag();
		for (RecommendedItem recommendation : recommendations) {
			Tuple tuple = tupleFactory.newTuple(2);
			tuple.set(0, recommendation.getItemID());
			tuple.set(1, recommendation.getValue());
			recommendationsBag.add(tuple);
		}
		return recommendationsBag;
	}

	public Schema outputSchema(Schema input) {
		Schema tupleSchema = new Schema();
		tupleSchema.add(new Schema.FieldSchema(null, DataType.LONG));
		tupleSchema.add(new Schema.FieldSchema(null, DataType.FLOAT));
		return tupleSchema;
	}
}