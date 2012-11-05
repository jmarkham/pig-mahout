package com.hortonworks.pig.mahout.recommend;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.mahout.cf.taste.common.NoSuchItemException;
import org.apache.mahout.cf.taste.common.NoSuchUserException;
import org.apache.mahout.cf.taste.common.Refreshable;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.FastByIDMap;
import org.apache.mahout.cf.taste.impl.common.FastIDSet;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveArrayIterator;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.impl.model.AbstractDataModel;
import org.apache.mahout.cf.taste.impl.model.GenericDataModel;
import org.apache.mahout.cf.taste.impl.model.GenericItemPreferenceArray;
import org.apache.mahout.cf.taste.impl.model.GenericPreference;
import org.apache.mahout.cf.taste.impl.model.GenericUserPreferenceArray;
import org.apache.mahout.cf.taste.model.Preference;
import org.apache.mahout.cf.taste.model.PreferenceArray;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;

public final class BagDataModel extends AbstractDataModel {

	private static final long serialVersionUID = -1864293167038999644L;
	private static final Log log = LogFactory.getLog(BagDataModel.class);

	private FastIDSet userIDs;
	private FastByIDMap<PreferenceArray> preferenceFromUsers;
	private FastIDSet itemIDs;
	private FastByIDMap<PreferenceArray> preferenceForItems;
	private FastByIDMap<FastByIDMap<Long>> timestamps;

	public BagDataModel(DataBag bag) {

		try {
			preferenceFromUsers = new FastByIDMap<PreferenceArray>();
			timestamps = new FastByIDMap<FastByIDMap<Long>>();

			for (Iterator<Tuple> i = bag.iterator(); i.hasNext();) {
				Tuple tuple = i.next();
				processTuple(tuple);
			}

			this.userIDs = new FastIDSet();
			LongPrimitiveIterator it = preferenceFromUsers.keySetIterator();
			while (it.hasNext()) {
				userIDs.add(it.next());
			}
			log.info(String.format("Processed %d preferences", bag.size()));
			init();

		} catch (ExecException e) {
			String msg = "Error while initializing data model with "
					+ this.getClass().getSimpleName();
			log.error(msg, e);
		}
	}

	private void processTuple(Tuple tuple) throws ExecException {

		Long userID = (Long) tuple.get(0);
		Long itemID = (Long) tuple.get(1);
		Float preference = (Float) tuple.get(2);
		Long timestamp = (Long) tuple.get(3);

		PreferenceArray prefs = preferenceFromUsers.get(userID);

		boolean exists = false;
		if (prefs != null) {
			for (int i = 0; i < prefs.length(); i++) {
				if (prefs.getItemID(i) == itemID) {
					exists = true;
					prefs.setValue(i, preference);
					break;
				}
			}
		}

		if (!exists) {
			if (prefs == null) {
				prefs = new GenericUserPreferenceArray(1);
			} else {
				PreferenceArray newPrefs = new GenericUserPreferenceArray(
						prefs.length() + 1);
				for (int i = 0, j = 1; i < prefs.length(); i++, j++) {
					newPrefs.set(j, prefs.get(i));
				}
				prefs = newPrefs;
			}
			prefs.setUserID(0, userID);
			prefs.setItemID(0, itemID);
			prefs.setValue(0, preference);
			((FastByIDMap<PreferenceArray>) preferenceFromUsers).put(userID,
					prefs);
		}

		if (timestamp != 0) {
			FastByIDMap<Long> itemTimestamps = timestamps.get(userID);
			if (itemTimestamps == null) {
				itemTimestamps = new FastByIDMap<Long>();
				timestamps.put(userID, itemTimestamps);
			}
			itemTimestamps.put(itemID, timestamp);
		}
	}

	private void init() {

		FastByIDMap<Collection<Preference>> prefsForItems = new FastByIDMap<Collection<Preference>>();
		FastIDSet itemIDSet = new FastIDSet();
		int currentCount = 0;
		float maxPrefValue = Float.NEGATIVE_INFINITY;
		float minPrefValue = Float.POSITIVE_INFINITY;
		for (Map.Entry<Long, PreferenceArray> entry : preferenceFromUsers
				.entrySet()) {
			PreferenceArray prefs = entry.getValue();
			prefs.sortByItem();
			for (Preference preference : prefs) {
				long itemID = preference.getItemID();
				itemIDSet.add(itemID);
				Collection<Preference> prefsForItem = prefsForItems.get(itemID);
				if (prefsForItem == null) {
					prefsForItem = new ArrayList<Preference>();
					prefsForItems.put(itemID, prefsForItem);
				}
				prefsForItem.add(preference);
				float value = preference.getValue();
				if (value > maxPrefValue) {
					maxPrefValue = value;
				}
				if (value < minPrefValue) {
					minPrefValue = value;
				}
			}
			if (++currentCount % 10000 == 0) {
				log.info(String.format("Processed %d users", currentCount));
			}
		}
		log.info(String.format("Processed %d users", currentCount));

		setMinPreference(minPrefValue);
		setMaxPreference(maxPrefValue);

		this.itemIDs = itemIDSet;

		this.preferenceForItems = GenericDataModel.toDataMap(prefsForItems,
				false);

		for (Map.Entry<Long, PreferenceArray> entry : preferenceForItems
				.entrySet()) {
			entry.getValue().sortByUser();
		}
	}

	public LongPrimitiveArrayIterator getUserIDs() {
		return new LongPrimitiveArrayIterator(userIDs.toArray());
	}

	public PreferenceArray getPreferencesFromUser(long userID)
			throws NoSuchUserException {
		PreferenceArray prefs = preferenceFromUsers.get(userID);
		if (prefs == null) {
			throw new NoSuchUserException(userID);
		}
		return prefs;
	}

	public FastIDSet getItemIDsFromUser(long userID) throws TasteException {
		PreferenceArray prefs = getPreferencesFromUser(userID);
		int size = prefs.length();
		FastIDSet result = new FastIDSet(size);
		for (int i = 0; i < size; i++) {
			result.add(prefs.getItemID(i));
		}
		return result;
	}

	public LongPrimitiveArrayIterator getItemIDs() {
		return new LongPrimitiveArrayIterator(itemIDs.toArray());
	}

	public PreferenceArray getPreferencesForItem(long itemID)
			throws NoSuchItemException {
		PreferenceArray prefs = preferenceForItems.get(itemID);
		if (prefs == null) {
			throw new NoSuchItemException(itemID);
		}
		return prefs;
	}

	public Float getPreferenceValue(long userID, long itemID)
			throws TasteException {
		PreferenceArray prefs = getPreferencesFromUser(userID);
		int size = prefs.length();
		for (int i = 0; i < size; i++) {
			if (prefs.getItemID(i) == itemID) {
				return prefs.getValue(i);
			}
		}
		return null;
	}

	public Long getPreferenceTime(long userID, long itemID)
			throws TasteException {
		if (timestamps == null) {
			return null;
		}
		FastByIDMap<Long> itemTimestamps = timestamps.get(userID);
		if (itemTimestamps == null) {
			throw new NoSuchUserException(userID);
		}
		return itemTimestamps.get(itemID);
	}

	public int getNumItems() {
		return itemIDs.size();
	}

	public int getNumUsers() {
		return userIDs.size();
	}

	public int getNumUsersWithPreferenceFor(long itemID) {
		PreferenceArray prefs1 = preferenceForItems.get(itemID);
		return prefs1 == null ? 0 : prefs1.length();
	}

	public int getNumUsersWithPreferenceFor(long itemID1, long itemID2) {
		PreferenceArray prefs1 = preferenceForItems.get(itemID1);
		if (prefs1 == null) {
			return 0;
		}
		PreferenceArray prefs2 = preferenceForItems.get(itemID2);
		if (prefs2 == null) {
			return 0;
		}

		int size1 = prefs1.length();
		int size2 = prefs2.length();
		int count = 0;
		int i = 0;
		int j = 0;
		long userID1 = prefs1.getUserID(0);
		long userID2 = prefs2.getUserID(0);
		while (true) {
			if (userID1 < userID2) {
				if (++i == size1) {
					break;
				}
				userID1 = prefs1.getUserID(i);
			} else if (userID1 > userID2) {
				if (++j == size2) {
					break;
				}
				userID2 = prefs2.getUserID(j);
			} else {
				count++;
				if (++i == size1 || ++j == size2) {
					break;
				}
				userID1 = prefs1.getUserID(i);
				userID2 = prefs2.getUserID(j);
			}
		}
		return count;
	}

	public void removePreference(long userID, long itemID) {

		userIDs.remove(userID);
		itemIDs.remove(itemID);

		// User preferences
		List<Long> usersToRemoveInItems = new ArrayList<Long>();
		if (preferenceFromUsers.containsKey(userID)) {
			for (Preference p : preferenceFromUsers.get(userID)) {
				usersToRemoveInItems.add(p.getItemID());
			}
		}
		preferenceFromUsers.remove(userID);

		// Item preferences
		List<Long> itemsToRemoveInUsers = new ArrayList<Long>();
		if (preferenceForItems.containsKey(itemID)) {
			for (Preference p : preferenceForItems.get(itemID)) {
				itemsToRemoveInUsers.add(p.getUserID());
			}
		}
		preferenceForItems.remove(itemID);

		// 2nd round on Item preferences to remove refs to Users
		for (Long item : itemsToRemoveInUsers) {
			if (preferenceForItems.containsKey(item)) {
				List<Preference> newPi = new ArrayList<Preference>();
				for (Preference p : preferenceForItems.get(item)) {
					if (p.getUserID() != userID) {
						newPi.add(p);
					}
				}
				preferenceForItems.put(item, new GenericItemPreferenceArray(
						newPi));
			}
		}

		// 2nd round on User preferences to remove refs to Items
		for (Long user : usersToRemoveInItems) {
			if (preferenceFromUsers.containsKey(user)) {
				List<Preference> newPu = new ArrayList<Preference>();
				for (Preference p : preferenceFromUsers.get(user)) {
					if (p.getItemID() != itemID) {
						newPu.add(p);
					}
				}
				preferenceFromUsers.put(user, new GenericUserPreferenceArray(
						newPu));
			}
		}
	}

	public void setPreference(long userID, long itemID, float value) {

		userIDs.add(userID);
		itemIDs.add(itemID);

		setMinPreference(Math.min(getMinPreference(), value));
		setMaxPreference(Math.max(getMaxPreference(), value));

		Preference p = new GenericPreference(userID, itemID, value);

		// User preferences
		GenericUserPreferenceArray newUPref;
		if (preferenceFromUsers.containsKey(userID)) {
			PreferenceArray oldPref = preferenceFromUsers.get(userID);
			newUPref = new GenericUserPreferenceArray(oldPref.length() + 1);
			for (int i = 0; i < oldPref.length(); i++) {
				newUPref.set(i, oldPref.get(i));
			}
			newUPref.set(oldPref.length(), p);
		} else {
			newUPref = new GenericUserPreferenceArray(1);
			newUPref.set(0, p);
		}
		preferenceFromUsers.put(userID, newUPref);

		// Item preferences
		GenericItemPreferenceArray newIPref;
		if (preferenceForItems.containsKey(itemID)) {
			PreferenceArray oldPref = preferenceForItems.get(itemID);
			newIPref = new GenericItemPreferenceArray(oldPref.length() + 1);
			for (int i = 0; i < oldPref.length(); i++) {
				newIPref.set(i, oldPref.get(i));
			}
			newIPref.set(oldPref.length(), p);
		} else {
			newIPref = new GenericItemPreferenceArray(1);
			newIPref.set(0, p);
		}
		preferenceForItems.put(itemID, newIPref);
	}

	public void refresh(Collection<Refreshable> alreadyRefreshed) {
	}

	public boolean hasPreferenceValues() {
		return true;
	}
}