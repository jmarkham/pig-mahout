package com.hortonworks.pig.mahout.recommend;

import java.util.Collection;
import java.util.Iterator;

import org.apache.mahout.cf.taste.common.Refreshable;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.FastByIDMap;
import org.apache.mahout.cf.taste.impl.common.FastIDSet;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.impl.model.AbstractDataModel;
import org.apache.mahout.cf.taste.impl.model.GenericBooleanPrefDataModel;
import org.apache.mahout.cf.taste.impl.model.GenericDataModel;
import org.apache.mahout.cf.taste.impl.model.GenericPreference;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.model.Preference;
import org.apache.mahout.cf.taste.model.PreferenceArray;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class BagDataModel extends AbstractDataModel {

	private static final long serialVersionUID = -1864293167038999644L;
	private static final Logger log = LoggerFactory
			.getLogger(BagDataModel.class);

	private DataModel delegate;
	private final boolean hasPrefValues;

	public BagDataModel(DataBag bag) throws ExecException {
		Tuple firstRecord = bag.iterator().next();
		hasPrefValues = firstRecord.size() >= 3 && firstRecord.isNull(2);

		delegate = buildModel(bag);
	}

	private DataModel buildModel(DataBag bag) throws ExecException {

		FastByIDMap<FastByIDMap<Long>> timestamps = new FastByIDMap<FastByIDMap<Long>>();

		if (hasPrefValues) {
			FastByIDMap<Collection<Preference>> data = new FastByIDMap<Collection<Preference>>();
			processBag(bag, data, timestamps);
			return new GenericDataModel(GenericDataModel.toDataMap(data, true),
					timestamps);
		} else {
			FastByIDMap<FastIDSet> data = new FastByIDMap<FastIDSet>();
			processBagWithoutID(bag, data, timestamps);
			return new GenericBooleanPrefDataModel(data, timestamps);
		}
	}

	private void processBag(DataBag bag, FastByIDMap<?> data,
			FastByIDMap<FastByIDMap<Long>> timestamps) throws ExecException {

		log.info("Reading bag info...");
		int count = 0;

		Iterator<Tuple> it = bag.iterator();
		while (it.hasNext()) {
			Tuple tuple = it.next();
			if (!tuple.isNull()) {
				processTuple(tuple, data, timestamps);
				if (++count % 1000000 == 0) {
					log.info("Processed {} lines", count);
				}
			}
		}
		log.info("Read lines: {}", count);
	}

	@SuppressWarnings("unchecked")
	protected void processTuple(Tuple tuple, FastByIDMap<?> data,
			FastByIDMap<FastByIDMap<Long>> timestamps) throws ExecException {

		Long userID = (Long) tuple.get(0);
		Long itemID = (Long) tuple.get(1);
		Float preferenceValue = (Float) tuple.get(2);

		boolean hasTimestamp = false;
		Long timestamp = null;
		if (tuple.size() == 4) {
			hasTimestamp = !tuple.isNull(3);
			timestamp = hasTimestamp ? (Long) tuple.get(3) : null;
		}

		Object maybePrefs = data.get(userID);

		Collection<Preference> prefs = (Collection<Preference>) maybePrefs;

		if (!hasTimestamp && preferenceValue == null) {
			if (prefs != null) {
				Iterator<Preference> prefsIterator = prefs.iterator();
				while (prefsIterator.hasNext()) {
					Preference pref = prefsIterator.next();
					if (pref.getItemID() == itemID) {
						prefsIterator.remove();
						break;
					}
				}
			}

			removeTimestamp(userID, itemID, timestamps);

		} else {

			boolean exists = false;
			if (prefs != null) {
				for (Preference pref : prefs) {
					if (pref.getItemID() == itemID) {
						exists = true;
						pref.setValue(preferenceValue);
						break;
					}
				}
			}

			if (!exists) {
				if (prefs == null) {
					prefs = Lists.newArrayListWithCapacity(2);
					((FastByIDMap<Collection<Preference>>) data).put(userID,
							prefs);
				}
				prefs.add(new GenericPreference(userID, itemID, preferenceValue));
			}

			addTimestamp(userID, itemID, timestamp, timestamps);
		}
	}

	protected void processBagWithoutID(DataBag bag,
			FastByIDMap<FastIDSet> data,
			FastByIDMap<FastByIDMap<Long>> timestamps) throws ExecException {
		log.info("Reading file info...");
		int count = 0;
		Iterator<Tuple> it = bag.iterator();
		while (it.hasNext()) {
			Tuple tuple = it.next();
			if (!tuple.isNull()) {

				if (tuple.get(0) instanceof DataBag) {
					DataBag nestedBag = (DataBag) tuple.get(0);
					Iterator<Tuple> nestedIt = nestedBag.iterator();
					while (nestedIt.hasNext()) {
						Tuple nestedTuple = nestedIt.next();
						processTupleWithoutID(nestedTuple, data, timestamps);
					}
				} else {
					processTupleWithoutID(tuple, data, timestamps);
					if (++count % 100000 == 0) {
						log.info("Processed {} lines", count);
					}
				}
			}
		}
		log.info("Read lines: {}", count);
	}

	protected void processTupleWithoutID(Tuple tuple,
			FastByIDMap<FastIDSet> data,
			FastByIDMap<FastByIDMap<Long>> timestamps) throws ExecException {

		Long userID = (Long) tuple.get(0);
		Long itemID = (Long) tuple.get(1);
		boolean hasPreference = tuple.isNull(2);
		String preferenceValueString = hasPreference ? (String) tuple.get(2)
				: "";
		boolean hasTimestamp = false;
		Long timestamp = null;
		if (tuple.size() == 4) {
			hasTimestamp = !tuple.isNull(3);
			timestamp = hasTimestamp ? (Long) tuple.get(3) : null;
		}

		if (hasPreference && !hasTimestamp && preferenceValueString.isEmpty()) {
			// Then line is of form "userID,itemID,", meaning remove

			FastIDSet itemIDs = data.get(userID);
			if (itemIDs != null) {
				itemIDs.remove(itemID);
			}

			removeTimestamp(userID, itemID, timestamps);

		} else {

			FastIDSet itemIDs = data.get(userID);
			if (itemIDs == null) {
				itemIDs = new FastIDSet(2);
				data.put(userID, itemIDs);
			}
			itemIDs.add(itemID);

			addTimestamp(userID, itemID, timestamp, timestamps);

		}
	}

	private void addTimestamp(long userID, long itemID, long timestamp,
			FastByIDMap<FastByIDMap<Long>> timestamps) {
		if (timestamp != 0) {
			FastByIDMap<Long> itemTimestamps = timestamps.get(userID);
			if (itemTimestamps == null) {
				itemTimestamps = new FastByIDMap<Long>();
				timestamps.put(userID, itemTimestamps);
			}
			itemTimestamps.put(itemID, timestamp);
		}
	}

	private static void removeTimestamp(long userID, long itemID,
			FastByIDMap<FastByIDMap<Long>> timestamps) {
		FastByIDMap<Long> itemTimestamps = timestamps.get(userID);
		if (itemTimestamps != null) {
			itemTimestamps.remove(itemID);
		}
	}

	public LongPrimitiveIterator getUserIDs() throws TasteException {
		return delegate.getUserIDs();
	}

	public PreferenceArray getPreferencesFromUser(long userID)
			throws TasteException {
		return delegate.getPreferencesFromUser(userID);
	}

	public FastIDSet getItemIDsFromUser(long userID) throws TasteException {
		return delegate.getItemIDsFromUser(userID);
	}

	public LongPrimitiveIterator getItemIDs() throws TasteException {
		return delegate.getItemIDs();
	}

	public PreferenceArray getPreferencesForItem(long itemID)
			throws TasteException {
		return delegate.getPreferencesForItem(itemID);
	}

	public Float getPreferenceValue(long userID, long itemID)
			throws TasteException {
		return delegate.getPreferenceValue(userID, itemID);
	}

	public Long getPreferenceTime(long userID, long itemID)
			throws TasteException {
		return delegate.getPreferenceTime(userID, itemID);
	}

	public int getNumItems() throws TasteException {
		return delegate.getNumItems();
	}

	public int getNumUsers() throws TasteException {
		return delegate.getNumUsers();
	}

	public int getNumUsersWithPreferenceFor(long itemID) throws TasteException {
		return delegate.getNumUsersWithPreferenceFor(itemID);
	}

	public int getNumUsersWithPreferenceFor(long itemID1, long itemID2)
			throws TasteException {
		return delegate.getNumUsersWithPreferenceFor(itemID1, itemID2);
	}

	public void setPreference(long userID, long itemID, float value)
			throws TasteException {
		delegate.setPreference(userID, itemID, value);
	}

	public void removePreference(long userID, long itemID)
			throws TasteException {
		delegate.removePreference(userID, itemID);
	}

	public boolean hasPreferenceValues() {
		return delegate.hasPreferenceValues();
	}

	public void refresh(Collection<Refreshable> alreadyRefreshed) {
		delegate.refresh(alreadyRefreshed);
	}
}