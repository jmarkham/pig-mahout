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
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.Utils;

public class Recommendation extends EvalFunc<DataBag> implements Algebraic {

	private static BagFactory bagFactory = BagFactory.getInstance();
	private static TupleFactory tupleFactory = TupleFactory.getInstance();
	private static long userID;
	private static int howMany = 10;

	public Recommendation(String userID) {
		this.userID = Long.parseLong(userID);
	}

	public Recommendation() {
	}

	@Override
	public DataBag exec(Tuple record) throws IOException {
		DataBag bag = bagFactory.newDefaultBag();
		bag.add(record);
		try {
			return recommend(this.userID, bag);
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

		private String initialUserID;

		public Initial() {
		}

		public Initial(String userID) {
			initialUserID = userID;
		}

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

		private String intermediateUserID;

		public Intermediate() {
		}

		public Intermediate(String userID) {
			intermediateUserID = userID;
		}

		@Override
		public Tuple exec(Tuple input) throws IOException {
			DataBag bag = (DataBag) input.get(0);
			Tuple tuple = tupleFactory.newTuple(1);
			DataBag intermediateBag = bagFactory.newDefaultBag();
			for (Iterator<Tuple> i = bag.iterator(); i.hasNext();) {
				intermediateBag.add(i.next());
			}
			tuple.set(0, intermediateBag);
			return tuple;
		}
	}

	static public class Final extends EvalFunc<DataBag> {

		private String finalUserID;

		public Final() {
		}

		public Final(String userID) {
			finalUserID = userID;
		}

		@Override
		public DataBag exec(Tuple input) throws IOException {
			try {
				return recommend(Long.parseLong(finalUserID),
						unwrapBag(input, bagFactory.newDefaultBag()));
			} catch (TasteException e) {
				String msg = "Error while computing recommendations in "
						+ this.getClass().getSimpleName();
				throw new ExecException(msg, PigException.ERROR, e);
			}
		}
	}

	private static DataBag unwrapBag(Tuple input, DataBag finalBag)
			throws ExecException {

		Object elem = input.get(0);
		if (elem != null && elem instanceof DataBag) {
			DataBag nestedBag = (DataBag) elem;
			for (Iterator<Tuple> i = nestedBag.iterator(); i.hasNext();) {
				Tuple nestedTuple = i.next();
				for (Object field : nestedTuple.getAll()) {
					if (field instanceof DataBag) {
						unwrapBag(nestedTuple, finalBag);
					} else {
						finalBag.add(nestedTuple);
					}
				}

			}
		}

		return finalBag;
	}

	private static DataBag recommend(Long userID, DataBag bag)
			throws ExecException, TasteException {
		DataModel model = new BagDataModel(bag);
		GenericRecommender recommender = new GenericRecommender(model);
		List<RecommendedItem> recommendedItems = recommender.recommend(userID,
				howMany);
		DataBag recommendationsBag = bagFactory.newDefaultBag();
		Tuple recommendations = tupleFactory.newTuple(2);
		recommendations.set(0, userID);
		DataBag recommendedItemsBag = bagFactory.newDefaultBag();
		for (RecommendedItem recommendedItem : recommendedItems) {
			Tuple recommendedItemTuple = tupleFactory.newTuple(2);
			recommendedItemTuple.set(0, recommendedItem.getItemID());
			recommendedItemTuple.set(1,
					((Float) recommendedItem.getValue()).intValue());
			recommendedItemsBag.add(recommendedItemTuple);
		}
		recommendations.set(1, recommendedItemsBag);
		recommendationsBag.add(recommendations);
		return recommendationsBag;
	}

	public Schema outputSchema(Schema input) {
		try {
			String schemaString = "all_recommendations:bag{user_recommendations:tuple(user:long, recommendation:bag{recommended_item:tuple(item:int, value:int)})}";
			return Utils.getSchemaFromString(schemaString);
		} catch (Exception e) {
			String msg = "Error while parsing output schema "
					+ this.getClass().getSimpleName();
			log.error(msg, e);
			return null;
		}
	}
}