package hf.JavaPredictors;

import gnu.trove.iterator.TIntIterator;
import gnu.trove.iterator.TObjectIntIterator;
import gnu.trove.map.hash.*;
import gnu.trove.set.hash.TIntHashSet;
import hf.JavaUtils.Pair;
import hf.JavaUtils.WordPair;
import hu.szaladas.botools.Counter;
import onlab.core.Database;
import onlab.core.Database.Event;
import onlab.core.Database.Item;
import onlab.core.Database.User;
import onlab.core.ExtendedDatabase;
import onlab.core.evaluation.Evaluation;
import onlab.core.predictor.Predictor;

import java.io.File;
import java.io.PrintWriter;
import java.sql.*;
import java.util.*;
import java.util.Map.Entry;

/**
 * @author DaveXster
 */
public class ExamplePredictor extends Predictor {

    public static final String META_KEYS = "metaKeys";
    private static final String KEYVALUE_SEP = " ";
    private static final String KEYVALUE_WEIGHT_SEP = ":";
    private static final String KEY_VALUE_LIST_SEPARATOR = "[^\\p{L}0-9:_ ]";
    public static final int LAMBDA = 10;
    public static final int TESTLIMIT = -1;
    private static final String itemPairSimsFileName = "itemPairSims.txt";
    private static final String wordPairSuppsFileName = "wordPairSupps.txt";
    private static final String itemPairSuppsFileName = "itemPairSupps.txt";
    private static final String metaWordSuppsByItemFileName = "metaWordSuppsByItem.txt";
    private static final String metaWordSuppsByEventFileName = "metaWordSuppsByEvent.txt";
    private static final String metaWordIndicesFileName = "metaWordIndices.txt";
    private static final String itemSuppsFileName = "itemSupps.txt";

    private String[] keys;
    private TCharDoubleHashMap weights;  //A-Actor, D-Director, V-VodMenu


    /**
     * supp(item) minden Item-re
     */
    private TIntObjectHashMap<TIntHashSet> itemsSeenByUsers;

    private TIntObjectHashMap<TIntHashSet> itemToItemSets;
    /**
     * index minden metaWord-re
     */
    public TObjectIntHashMap<String> metaWords;
    /**
     * supp(metaWord) minden metaWord-re
     */
    public TObjectIntHashMap<String> metaWordSuppsByItems;

    public TObjectIntHashMap<String> metaWordSuppsByEvents;

    public Counter itemCounter;

    /**
     * supp(A,B) minden A,B-re
     */
    public Map<Pair, Integer> itemPairSupps;

    public TObjectIntHashMap<Pair> itemToItemLinkCounters;

    /**
     * supp(W); W - VodMenu, Actor vagy Director el?fordul?sa
     */
    public Map<Pair, Double> wordPairSupps;

    public Map<WordPair, Integer> wordPairSupps2;

    /**
     * sim(A,B) minden A,B-re
     */
    public Map<Pair, Double> itemSims;

    @Override
    protected void initParameters() {
        par.put(META_KEYS, "");
    }

    public ExtendedDatabase dbExt = null;

    @Override
    public void train(Database db, Evaluation eval) {
        System.out.println("Started training...");
        this.db = db;

        dbExt = (ExtendedDatabase) db;

        String[] keys0 = par.get(META_KEYS).split(KEYVALUE_SEP);
        keys = new String[keys0.length];
        weights = new TCharDoubleHashMap(keys0.length);

        for (int i = 0; i < keys.length; i++) {
            String[] kv = keys0[i].split(KEYVALUE_WEIGHT_SEP);
            keys[i] = kv[0];
            weights.put(kv[0].charAt(0), Double.parseDouble(kv[1]));
        }

        countItemSupps();
        //printSuppsToFile("itemPairSupps");

        itemsSeenByUsers = null;

        //indexKeyValues(false, false);       //metaWord indexelés, supp(mW) számítás itemek alapján

        //countMetaWordSuppsByEvents();   //supp(mW) számítás eventek alapján
//
//		countItemPairSupps();
//		writeitemPairSuppsToFile();


//		countMetaWordPairSuppsAll();
//		printMetaWordPairSuppsByEvents();

//		System.exit(-1);

        //countItemSupps(); 			// melyik filmet h?nyszor v?ltott?k ki

        //printSuppsToFile(); 			//el?fordul?sok f?jlba ?r?sa

        //ha nincs m?g meg a sim(A,B) sz?m?t?s
//		if(!isSimsFileExists()){
//			// minden item kombin?ci?ra kisz?molni a supp(a,b)-t
//			countItemPairSupps();
//
//			/* Ha van el?g mem?ria */
//			//computeItemSims();
//
//			/* Ha nincs el?g mem?ria, akkor automatikusan f?jlba ?rjuk */
//			computeItemSimsToFile();
//
//			//m?r nincs r? sz?ks?gem
//			itemPairSupps = null;
//		}

		/* kapott kombin?ci?k el?fordul?s?nak f?jlba ?r?sa */
        // writeitemPairSuppsToFile("itemPairSupps.txt");

        //countMetaWordPairSupps();
        //countMetaWordPairSuppsAllToFile();

        // computeItemSimsFromDB("jdbc:oracle:thin:@localhost:1521:xe","system","********");

        //System.gc();
        //loaditemPairSimsFromFile();

//		System.out.println("Other functions...");
//		System.out.println("Number of events: " + db.numEvents());
//		System.out.println("Number of items: " + db.numItems());
//		System.out.println("Number of users: " + db.numUsers());
//		System.out.println("Max time of train DB: " + db.getMaxTimeByEvents());
//		System.out.println("Min time of train DB: " + db.getMinTimeByEvents());
    }

    /**
     * metaWord indexelő
     *
     * @param countMetaWordsByItem Szamoljon-e supp(mW)-öt itemek alapjan?
     */
    public void indexKeyValues(boolean printIndices, boolean countMetaWordsByItem) {
        metaWords = new TObjectIntHashMap<String>(10500);
        if (countMetaWordsByItem) metaWordSuppsByItems = new TObjectIntHashMap<String>(10500);

        int seq = 0;
        for (Item i : db.items(null)) {
            for (String key : keys) {
                String keyAll = dbExt.getItemKeyValue(i.idx, key);
                String[] values = keyAll.split(KEY_VALUE_LIST_SEPARATOR); // /f és / jel menten felbontva
                char keyType = key.charAt(0);
                for (String val : values) {
                    if (val.equals(""))
                        continue;
                    String mWrd = keyType + ":" + val.toLowerCase();
                    if (!metaWords.containsKey(mWrd)) {
                        seq++;
                        metaWords.put(mWrd, seq);
                    }
                    if (countMetaWordsByItem) {
                        if (!metaWordSuppsByItems.containsKey(mWrd)) {
                            metaWordSuppsByItems.put(mWrd, 1);
                        } else {
                            metaWordSuppsByItems.put(mWrd, metaWordSuppsByItems.get(mWrd) + 1);
                        }
                    }
                }
            }
        }
        if (printIndices) printSuppsToFile("metaWordIndices");
        if (countMetaWordsByItem) printSuppsToFile("wordSuppsByItems");
    }


    public void loadItemSuppsFromFile() {
        System.out.println("Loading item Similarities...");
        int importedRows = 0;
        int percent = 1;
        //erre az adatb?zisra ~17 milli? itemhasonl?s?g van sz?m?tva
        //ami nincs ksiz?m?tva, az 0
        if (itemSims == null) {
            itemSims = new HashMap<Pair, Double>(23000000, 1.0f);
        }
        Scanner s = null;
        try {
            s = new Scanner(new File(itemPairSimsFileName));
            while (s.hasNextLine()) {
                String[] line = s.nextLine().split(" ");
                int rowFirstItemID = Integer.parseInt(line[0]);
                int rowSecondItemID = Integer.parseInt(line[1]);
                Double d = Double.valueOf(line[2]);
                itemSims.put(new Pair(rowFirstItemID, rowSecondItemID), d);
                importedRows++;
                if (importedRows % 163070 == 0) {
                    System.out.print(percent + "%  ");
                    percent++;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            s.close();
            System.out.println("Loading item Similarities failed!");
        } finally {
            s.close();
        }
        System.out.println("Loading item Similarities done!");
    }


    public boolean isSimsFileExists() {
        File f = new File(itemPairSimsFileName);
        if (f.exists() && f.isFile()) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * Eventek alapjan itemSupport, és item-to-item support szamitas
     */
    public void countItemSupps() {
        System.out.println("Item pair support szamitasa...");
        int nEvents = 0;
        int percent = 1;
		int size = db.numEvents();
		int percent1 = size / 100;
        itemsSeenByUsers = new TIntObjectHashMap(db.numUsers());
        itemToItemSets = new TIntObjectHashMap<TIntHashSet>();
        itemToItemLinkCounters = new TObjectIntHashMap<Pair>();
        for (Event e : db.events(null)) {
            nEvents++;
            int iIdx = e.iIdx;
            int uIdx = e.uIdx;
            TIntHashSet items;
            if (!itemsSeenByUsers.containsKey(uIdx)) {
                items = new TIntHashSet();
                itemsSeenByUsers.put(uIdx, items);
            } else {
                items = itemsSeenByUsers.get(uIdx);
            }

            if (!items.contains(iIdx)) {
                // ami benne volt:
                TIntIterator iterator = items.iterator();
                while (iterator.hasNext()) {
                    int item = iterator.next();
                    linkItems(item, iIdx);
                }

                items.add(iIdx);
            }
            if (nEvents == percent1) {
                System.out.print(percent + "% ");
                percent++;
                nEvents = 0;
            }
        }
    }

    private void linkItems(int item1, int item2) {
        getLinkedItems(item1).add(item2);
        getLinkedItems(item2).add(item1);

        Pair pair = new Pair(item1, item2);
        if(itemToItemLinkCounters.containsKey(pair)) {
            itemToItemLinkCounters.put(pair, itemToItemLinkCounters.get(pair) + 1);
        } else {
            itemToItemLinkCounters.put(pair, 1);
        }
    }

    private TIntHashSet getLinkedItems(int item) {
        if (itemToItemSets.contains(item)) {
            return itemToItemSets.get(item);
        } else {
            TIntHashSet linkedItems = new TIntHashSet();
            itemToItemSets.put(item, linkedItems);
            return linkedItems;
        }
    }


    /**
     * supp(A,B) minden A,B-re! Eredm?ny az itemPairSupps-ban t?rolva.
     */
    public void countItemPairSupps() {
//		itemPairSupps = new HashMap<Pair, Integer>();
//		System.out.println("Iterating on users and events...");
//		System.out.print("Users processed:");
//		int nUsers = 0;
//		int percent = 1;
//		int size = db.numUsers();
//		int percent1 = size / 100;
//		for (User u : db.users(null)) {
//			nUsers++;
//			if (TESTLIMIT == -1 || nUsers < TESTLIMIT) {
//				List<Integer> ue = new ArrayList<Integer>();
//				for (Event e : db.userEvents(u, null)) {
//                    e.
//					ue.add(e.iIdx);
//				}
//				for (int i = 0; i < ue.size(); i++) {
//					int idx1 = ue.get(i);
//					for (int j = i + 1; j < ue.size(); j++) {
//						int idx2 = ue.get(j);
//						if (idx1 != idx2) {
//							Pair mp = new Pair(idx1, idx2);
//							if (itemPairSupps.containsKey(mp)) {
//								int temp = itemPairSupps.get(mp) + 1;
//								itemPairSupps.put(mp, temp);
//							} else {
//								itemPairSupps.put(mp, 1);
//							}
//						}
//					}
//				}
//				if (nUsers % percent1 == 0) {
//					System.out.print(percent + "% ");
//					percent++;
//				}
//			}
//		}
//		System.out.println();
//		System.out.println("nUsers: " + nUsers);
    }


    /**
     * sim(A,B) = supp(A,B) / [sqrt(supp(A) + lambda) * sqrt(supp(B) + lambda)]
     */
    public void computeItemSims() {
        System.out.println("Computing item Similarities...");
        itemSims = new HashMap<Pair, Double>();
        List<Integer> items = new ArrayList<Integer>(db.numItems());
        for (Item i : db.items(null)) {
            items.add(i.idx);
        }
        items.sort(null);
        int size = items.size();
        int percent1 = size / 100;
        int percent = 0;
        int nItem = 0;

        for (int i = 0; i < items.size(); i++) {
            nItem++;
            if (TESTLIMIT == -1 || nItem < TESTLIMIT) {

                for (int j = i + 1; j < items.size(); j++) {
                    if (TESTLIMIT == -1 || nItem < TESTLIMIT) {
                        Pair mp = new Pair(items.get(i), items.get(j));
                        int suppPair = itemPairSupps.get(mp) == null ? 0
                                : itemPairSupps.get(mp);
                        int suppA = itemCounter.getCount(items.get(i));
                        int suppB = itemCounter.getCount(items.get(j));
                        Double sim;
                        sim = suppPair
                                / (double) (Math.sqrt(suppA + LAMBDA) * Math
                                .sqrt(suppB + LAMBDA));
                        itemSims.put(mp, sim);
                    }
                }
            }
            if (nItem % percent1 == 0) {
                System.out.print(percent + "% ");
                percent++;
            }
        }
        System.out.println();
        System.out.println("nItems: " + nItem);
    }



    public void computeItemSimsFromDB(String connectionString, String userName,
                                      String pw) {
        itemSims = new HashMap<Pair, Double>();
        List<Integer> items = new ArrayList<Integer>(db.numItems());
        for (Item i : db.items(null)) {
            items.add(i.idx);
        }
        items.sort(null);
        int size = items.size();
        int percent = 1;
        int percent2 = size / 100;

        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
        } catch (ClassNotFoundException e) {
            System.out.println("No Oracle JDBC Driver found!");
            e.printStackTrace();
            return;
        }
        System.out.println("Oracle JDBC Driver Registered!");
        Connection connection = null;
        try {
            connection = DriverManager.getConnection(connectionString,
                    userName, pw);
        } catch (SQLException e) {
            System.out.println("Connection Failed!");
            e.printStackTrace();
            return;
        }
        if (connection != null) {
            System.out.println("Connected!");
        } else {
            System.out.println("Failed to make connection!");
        }

        for (int i = 0; i < size; i++) {
            System.out.println(i);
            for (int j = i + 1; j < size; j++) {
                int suppPair;
                Pair mp = new Pair(items.get(i), items.get(j));
                String sql = "Select cnt from ONLAB_ITEMPAIR_SUPPS WHERE ITEMID1 = "
                        + mp.iIdx1 + "AND ITEMID2 = " + mp.iIdx2;
                try {
                    PreparedStatement preStatement = connection
                            .prepareStatement(sql);
                    ResultSet result = preStatement.executeQuery();
                    suppPair = result.getInt("cnt");
                } catch (SQLException e) {
                    suppPair = 0;
                }

                int suppA;
                sql = "Select cnt from ONLAB_ITEM_SUPPS WHERE ITEMID1 = "
                        + mp.iIdx1;
                try {
                    PreparedStatement preStatement = connection
                            .prepareStatement(sql);
                    ResultSet result = preStatement.executeQuery();
                    suppA = result.getInt("cnt");
                } catch (SQLException e) {
                    suppA = 0;
                }

                int suppB;
                sql = "Select cnt from ONLAB_ITEM_SUPPS WHERE ITEMID2 = "
                        + mp.iIdx2;
                try {
                    PreparedStatement preStatement = connection
                            .prepareStatement(sql);
                    ResultSet result = preStatement.executeQuery();
                    suppB = result.getInt("cnt");
                } catch (SQLException e) {
                    suppB = 0;
                }
                Double sim = suppPair
                        / (double) (Math.sqrt(suppA + LAMBDA) * Math.sqrt(suppB
                        + LAMBDA));
                itemSims.put(mp, sim);
            }

            if (i % percent2 == 0) {
                System.out.print(percent + "% ");
                percent++;
            }
        }
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    public void printMetaWordPairSuppsByEvents() {
        System.out.println("Printing MetaWord Pair Supps to " + wordPairSuppsFileName);
        PrintWriter pOut = null;
        try {
            pOut = new PrintWriter(wordPairSuppsFileName);
            for (Entry<WordPair, Integer> entry : wordPairSupps2.entrySet()) {
                pOut.println(entry.getKey().word1 + "," + entry.getKey().word2 + "," + entry.getValue());
            }
        } catch (Exception e) {
            System.err.println(e.getMessage());
        } finally {
            pOut.close();
            System.out.println("Printing supps is successful!");
        }
    }


    public void countMetaWordPairSuppsForEvent(Pair p) {
        int iIdx1 = p.iIdx1;
        int iIdx2 = p.iIdx2;
        Set<WordPair> vodMenuPairSet = null;

        String[] keys = par.get(META_KEYS).split(KEYVALUE_SEP);

        for (String key : keys) {
            String[] keyvalues1 = dbExt.getItemKeyValue(iIdx1, key).split(
                    KEY_VALUE_LIST_SEPARATOR);
            String[] keyvalues2 = dbExt.getItemKeyValue(iIdx2, key).split(
                    KEY_VALUE_LIST_SEPARATOR);

            if (!key.equals("VodMenuDirect")) {
                for (String s1 : keyvalues1) {
                    if (!s1.equals("")) {
                        for (String s2 : keyvalues2) {
                            if (!s2.equals("")) {
                                if (!s1.equals(s2)) {
                                    WordPair wp = new WordPair(s1, s2);
                                    if (wordPairSupps2.containsKey(wp)) {
                                        int temp = wordPairSupps2.get(wp) + 1;
                                        wordPairSupps2.put(wp, temp);
                                    } else {
                                        wordPairSupps2.put(wp, 1);
                                    }
                                }
                            }
                        }
                    }
                }
            } else {
                vodMenuPairSet = new HashSet<WordPair>();
                for (String s1 : keyvalues1) {
                    if (!s1.equals("")) {
                        String[] tokens1 = s1.split("/");
                        for (String s2 : keyvalues2) {
                            if (!s2.equals("")) {
                                String[] tokens2 = s2.split("/");
                                for (String tok1 : tokens1) {
                                    for (String tok2 : tokens2) {
                                        if (!tok1.equals(tok2)) {
                                            WordPair wp = new WordPair(tok1,
                                                    tok2);
                                            vodMenuPairSet.add(wp);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                for (WordPair wp : vodMenuPairSet) {
                    if (wordPairSupps2.containsKey(wp)) {
                        int temp = wordPairSupps2.get(wp) + 1;
                        wordPairSupps2.put(wp, temp);
                    } else {
                        wordPairSupps2.put(wp, 1);
                    }
                }

            }
        }
    }

    /**
     * To compute MetaWord pair supps based on eventlist
     */
    public void countMetaWordPairSuppsAll() {
        System.out.println("Counting metadata word pair supps...");
        wordPairSupps2 = new HashMap<WordPair, Integer>(10000000);
        int nUsers = 0;
        int percent = 1;
        int size = db.numUsers();
        int percent1 = size / 100;
        for (User u : db.users(null)) {
            nUsers++;
            if (TESTLIMIT == -1 || nUsers < TESTLIMIT) {
                List<Integer> ue = new ArrayList<Integer>();
                for (Event e : db.userEvents(u, null)) {
                    ue.add(e.iIdx);
                }
                for (int i = 0; i < ue.size(); i++) {
                    int idx1 = ue.get(i);
                    for (int j = i + 1; j < ue.size(); j++) {
                        int idx2 = ue.get(j);
                        if (idx1 != idx2) {
                            Pair mp = new Pair(idx1, idx2);
                            countMetaWordPairSuppsForEvent(mp);
                        }
                    }
                }
                if (nUsers % percent1 == 0) {
                    System.out.print(percent + "% ");
                    percent++;
                }
            }
        }
        System.out.println();
        System.out.println("Done!");
    }


    public void countMetaWordPairSuppsAllToFile() {
//        System.out.println("Counting metadata word pair supps...");
//        ExtendedDatabase dbExt = (ExtendedDatabase) db;
//        wordPairSupps = new HashMap<Pair, Double>();
//        List<Integer> items = new ArrayList<Integer>(db.numItems());
//        for (Item i : db.items(null)) {
//            items.add(i.idx);
//        }
//        items.sort(null);
//        int nItems = 0;
//        int percent = 1;
//        int size = items.size();
//        int percent1 = size / 100;
//        int numWords = 0;
//        double sim = 0.0;
//        int i, j;
//        i = j = 0;
//        String[] keys = par.get(META_KEYS).split(KEYVALUE_SEP);
//        PrintWriter pOut = null;
//        try {
//            pOut = new PrintWriter(wordPairSuppsFileName);
//            for (i = 0; i < items.size(); i++) {
//                if (TESTLIMIT == -1 || nItems < TESTLIMIT) {
//                    // adott item adatainak kiment?se, hogy ne kelljen ism?telten
//                    // lek?rdezni
//                    int iIdx1 = items.get(i);
//                    nItems++;
//                    String[] actor = dbExt.getItemKeyValue(iIdx1, "Actor").split(
//                            KEY_VALUE_LIST_SEPARATOR);
//                    String[] director = dbExt.getItemKeyValue(iIdx1, "Director")
//                            .split(KEY_VALUE_LIST_SEPARATOR);
//                    String[] vodmenu = dbExt
//                            .getItemKeyValue(iIdx1, "VodMenuDirect").split(
//                                    KEY_VALUE_LIST_SEPARATOR);
//
//                    for (j = i + 1; j < items.size(); j++) {
//                        if (TESTLIMIT == -1 || nItems < TESTLIMIT) {
//
//                            int iIdx2 = items.get(j);
//
//                            for (String key : keys) {
//
//                                String[] vals1;
//                                if (key.equals("VodMenuDirect")) {
//                                    vals1 = vodmenu;
//                                } else if (key.equals("Actor")) {
//                                    vals1 = actor;
//                                } else {
//                                    vals1 = director;
//                                }
//                                String valExpr2 = dbExt.getItemKeyValue(iIdx2, key);
//                                String[] vals2 = valExpr2
//                                        .split(KEY_VALUE_LIST_SEPARATOR);
//                                for (String val1 : vals1) {
//                                    for (String val2 : vals2) {
//                                        if (!key.equals("VodMenuDirect")) {
//                                            numWords++;
//                                            if (val1.equals(val2)) { // ha a sz?n?sz
//                                                // vagy
//                                                // rendez?
//                                                // ugyanaz
//                                                sim += ActorDirectorWeight;
//                                            }
//                                        } else {
//                                            String[] tokens1 = val1.split("/"); // VODMenu
//                                            // tokenek
//                                            String[] tokens2 = val2.split("/");
//                                            for (String tok1 : tokens1) {
//                                                numWords++;
//                                                for (String tok2 : tokens2) {
//                                                    if (tok1.equals(tok2)) {
//                                                        sim += VODMenuWordWeight;
//                                                    }
//                                                }
//                                            }
//                                        }
//                                    }
//                                }
//                            }
//                            Pair p = new Pair(iIdx1, iIdx2);
//                            sim = sim / (double) numWords;
//                            pOut.println(p.iIdx1 + " " + p.iIdx2 + " " + sim);
//                            sim = 0;
//                            numWords = 0;
//                        }
//                    }
//                }
//                if (nItems % percent1 == 0) {
//                    System.out.print(percent + "%  ");
//                    percent++;
//                }
//                pOut.flush();
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        } finally {
//            pOut.close();
//        }
    }


    public double countMetaWordPairSupps(Pair p) {
        double sim = 0.0;
//        int iIdx1 = p.iIdx1;
//        int iIdx2 = p.iIdx2;
//        ExtendedDatabase dbExt = (ExtendedDatabase) db;
//        wordPairSupps = new HashMap<Pair, Double>();
//        int numWords = 0;
//
//        String[] keys = par.get(META_KEYS).split(KEYVALUE_SEP);
//        Map words1 = new HashMap<String, List<String>>();
//        Map words2 = new HashMap<String, List<String>>();
//        for (String key : keys) {
//            List<String> list1 = new ArrayList<String>();
//            List<String> list2 = new ArrayList<String>();
//            String valExpr1 = dbExt.getItemKeyValue(iIdx1, key);
//            String[] vals1 = valExpr1.split(KEY_VALUE_LIST_SEPARATOR);
//            String valExpr2 = dbExt.getItemKeyValue(iIdx2, key);
//            String[] vals2 = valExpr2.split(KEY_VALUE_LIST_SEPARATOR);
//
//            if (!key.equals("VodMenuDirect")) {
//                for (String val1 : vals1) {
//                    if (!val1.equals("")) {
//                        list1.add(val1);
//                    }
//                }
//                for (String val2 : vals2) {
//                    if (!val2.equals("")) {
//                        list2.add(val2);
//                    }
//                }
//                words1.put(key, list1);
//                words2.put(key, list2);
//            } else if (key.equals("VodMenuDirect")) {
//                for (String val1 : vals1) {
//                    String[] tokens1 = val1.split("/");
//                    for (String tok1 : tokens1) {
//                        if (!tok1.equals("") && !list1.contains(tok1)) {
//                            list1.add(tok1);
//                        }
//                    }
//                }
//                for (String val2 : vals2) {
//                    String[] tokens2 = val2.split("/");
//                    for (String tok2 : tokens2) {
//                        if (!tok2.equals("") && !list1.contains(tok2)) {
//                            list2.add(tok2);
//                        }
//                    }
//                }
//                words1.put(key, list1);
//                words2.put(key, list2);
//            }
//        }
//        for (String key : keys) {
//            List<String> l1 = (List<String>) words1.get(key);
//            List<String> l2 = (List<String>) words2.get(key);
//            numWords += l1.size() + l2.size();
//            for (String s1 : l1) {
//                for (String s2 : l2) {
//                    if (s1.equals(s2)) {
//                        if (key.equals("VodMenuDirect")) {
//                            sim += VODMenuWordWeight;
//                        } else {
//                            sim += ActorDirectorWeight;
//                        }
//                    }
//                }
//            }
//        }
//        if (numWords == 0) {
//            return 0.0;
//        }
//        sim = sim / (double) (numWords - sim);
        return sim;
    }


    public void printSuppsToFile(String type) {
        PrintWriter out = null;
        String s = new String("'");
        if (type.equals("itemSupp")) {
            try {
                System.out.println("Printing support of items...");
                out = new PrintWriter(itemSuppsFileName);
                for (Object item : itemCounter.keySet()) {

                    out.println(item + "\t" + itemCounter.getCount(item));
                }
            } catch (Exception e) {
                System.err.println(e.getMessage());
            } finally {
                out.close();
                System.out.println("Printing supps is successful!");
            }
        } else if (type.equals("metaWordSuppsByEvents")) {
            try {
                System.out.println("Printing support of metawords...");
                out = new PrintWriter(metaWordSuppsByEventFileName);
                TObjectIntIterator<String> iterator = metaWordSuppsByEvents.iterator();
                while (iterator.hasNext()) {
                    iterator.advance();
                    out.println(iterator.key() + ":\t\t\t\t" + iterator.value());
                }
            } catch (Exception e) {
                System.err.println(e.getMessage());
            } finally {
                out.close();
                System.out.println("Printing metaword supps is successful!");
            }
        } else if (type.equals("wordSuppsByItems")) {
            try {
                System.out.println("Printing support of metawords...");
                out = new PrintWriter(metaWordSuppsByItemFileName);
                TObjectIntIterator<String> iterator = metaWordSuppsByItems.iterator();
                while (iterator.hasNext()) {
                    iterator.advance();
                    out.println(iterator.key() + ":\t\t\t\t" + iterator.value());
                }
            } catch (Exception e) {
                System.err.println(e.getMessage());
            } finally {
                out.close();
                System.out.println("Printing metaword supps is successful!");
            }
        } else if (type.equals("metaWordIndices")) {
            char apos = (char) 39;
            try {
                System.out.println("Printing indices of metawords...");
                out = new PrintWriter(metaWordIndicesFileName);
                TObjectIntIterator<String> iterator = metaWords.iterator();
                while (iterator.hasNext()) {
                    iterator.advance();
                    out.println(iterator.key() + ":\t\t\t\t" + iterator.value());
                }
            } catch (Exception e) {
                System.err.println(e.getMessage());
            } finally {
                out.close();
                System.out.println("Printing metaword indices is successful!");
            }
        } else if (type.equals("itemPairSupps")) {
            try {
                System.out.println("Printing support of itempairs into file " + itemPairSuppsFileName);
                out = new PrintWriter(itemPairSuppsFileName);
                TObjectIntIterator<Pair> iterator = itemToItemLinkCounters.iterator();
                while (iterator.hasNext()) {
                    iterator.advance();
                    Pair p = iterator.key();
                    out.println(p.iIdx1 + "\t" + p.iIdx2 + "\t" + iterator.value());
                }
            } catch (Exception e) {
                System.err.println(e.getMessage());
            } finally {
                out.close();
                System.out.println("Printing itempairsupps is successful!");
            }
        }
    }

    public void loaditemPairSimsFromFile() {
        System.out.println("Loading item Similarities...");
        int importedRows = 0;
        int percent = 1;
        //erre az adatb?zisra ~17 milli? itemhasonl?s?g van sz?m?tva
        //ami nincs ksiz?m?tva, az 0
        if (itemSims == null) {
            itemSims = new HashMap<Pair, Double>(23000000, 1.0f);
        }
        Scanner s = null;
        try {
            s = new Scanner(new File(itemPairSimsFileName));
            while (s.hasNextLine()) {
                String[] line = s.nextLine().split(" ");
                int rowFirstItemID = Integer.parseInt(line[0]);
                int rowSecondItemID = Integer.parseInt(line[1]);
                Double d = Double.valueOf(line[2]);
                itemSims.put(new Pair(rowFirstItemID, rowSecondItemID), d);
                importedRows++;
                if (importedRows % 163070 == 0) {
                    System.out.print(percent + "%  ");
                    percent++;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            s.close();
            System.out.println("Loading item Similarities failed!");
        } finally {
            s.close();
        }
        System.out.println("Loading item Similarities done!");
    }

    /**
     * A param?ter?l kapott (A,B) p?rokra visszaadja a sim(A,B) ?rt?ket
     *
     * @param pairs
     * @return
     */
    public Map<Pair, Double> getItemPairSim(List<Pair> pairs) {
        List<Pair> pairs2 = pairs;
        Map<Pair, Double> map = new HashMap<Pair, Double>();

        if (this.itemSims != null) { // nem f?jlba ?rtunk
            for (Pair p : pairs2) {
                Double val = itemSims.get(p) == null ? 0.0 : itemSims.get(p);
                map.put(p, val);
            }
            return map;
        } else { // ha f?jlba ?rtunk
            Scanner s = null;
            try {
                s = new Scanner(new File(itemPairSimsFileName));
                while (s.hasNextLine() && pairs2.size() > 0) {
                    String[] line = s.nextLine().split(" ");
                    int rowFirstItemID = Integer.parseInt(line[0]);
                    int rowSecondItemID = Integer.parseInt(line[1]);
                    List<Pair> temp = new ArrayList<Pair>(pairs2);
                    for (Pair p : pairs2) {
                        // az itemPairSims bemeneti f?jl az els? index szerint
                        // rendezett
                        // ha az els? index?nk nagyobb, mint a beolvasott els?
                        // index, akkor nincs tal?lat
                        if (rowFirstItemID > p.iIdx1) {
                            map.put(p, 0.0);
                            temp.remove(p);
                        }
                        //ha az elso index egyezik, de a m?sodik m?r nagyobb a fileban, nincs tal?lta
                        else if (rowFirstItemID == p.iIdx1 && rowSecondItemID > p.iIdx2) {
                            map.put(p, 0.0);
                            temp.remove(p);
                        }
                        //ha mindk?t index egyezik, tal?lta
                        else if (Integer.parseInt(line[0]) == p.iIdx1
                                && Integer.parseInt(line[1]) == p.iIdx2) {
                            map.put(p, Double.valueOf(line[2]));
                            temp.remove(p);
                        }
                    }
                    pairs2 = temp;
                }
                return map;
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                s.close();
            }
            return null;
        }
    }

    public Map<Pair, Double> getWordPairSims(List<Pair> pairs) {
        Map<Pair, Double> map = new HashMap<Pair, Double>();
        for (Pair p : pairs) {
            double sim = countMetaWordPairSupps(p);
            map.put(p, sim);
        }
        return map;
    }

    public static Comparator<Event> EventTimeComparator = new Comparator<Event>() {
        public int compare(Event event1, Event event2) {
            return Long.compare(event2.time, event1.time);
        }
    };


    @Override
    public double predict(int uIdx, int iIdx, long time) {

        double prediction = 0.5;

//        TObjectDoubleHashMap<String> profileWeights = createUserMetaWordProfile(uIdx);
//
//        for (String key : keys) {
//            String keyAll = dbExt.getItemKeyValue(iIdx, key);
//            String[] values = keyAll.split(KEY_VALUE_LIST_SEPARATOR); // /f és / jel menten felbontva
//            for (String val : values) {
//                if (val.equals(""))
//                    continue;
//                char c = key.charAt(0);
//                String mWrd = c + ":" + val.toLowerCase();
//                if (profileWeights.containsKey(mWrd)) {
//                    prediction = prediction + profileWeights.get(mWrd) * weights.get(c);
//                }
//            }
//        }

        return prediction;


        // System.out.println("Predict u" + uIdx + " and item " + iIdx);
//		double predictedValue = 0.0;
//		if (db.getItem(iIdx) == null) {
//			return predictedValue;
//		}
//		int n = 0;
//		List<Pair> pairs = new ArrayList<Pair>();
//		switch (PREDICTYPE) {
//		case 0: {	//random 5 db
//			for (Event e : db.userEvents(uIdx, null)) {
//				pairs.add(new Pair(e.iIdx, iIdx));
//				n++;
//				if (n >= EVENTNUM) {
//					break;
//				}
//			}
//			break;
//		}
//		case 1: {	//legutols? 5 db
//			List<Event> events = new ArrayList<Event>();
//			for (Event e : db.userEvents(uIdx, null)) {
//				events.add(e);
//			}
//			events.sort(this.EventTimeComparator);
//			for (Event e : events) {
//				pairs.add(new Pair(e.iIdx, iIdx));
//				n++;
//				if (n >= EVENTNUM) {
//					break;
//				}
//			}
//			break;
//		}
//		default: {	//?sszes
//			for (Event e : db.userEvents(uIdx, null)) {
//				pairs.add(new Pair(e.iIdx, iIdx));
//				n++;
//			}
//			break;
//		}
//		}
//		n = pairs.size();
//		if (n == 0) {
//			return predictedValue;
//		}
//		Map<Pair, Double> itemPairsims = getItemPairSim(pairs);
//		Map<Pair, Double> wordPairsims = getWordPairSims(pairs);
//		if (itemPairsims != null && wordPairsims != null) {
//			for (Pair p : pairs) {
//				predictedValue += (itemPairsims.get(p) == null ? 0.0
//						: itemPairsims.get(p).doubleValue())
//
//						+ (wordPairsims.get(p) == null ? 0.0 : wordPairsims
//								.get(p).doubleValue());
//			}
//			predictedValue = predictedValue / (double) n;
//		}
//		// System.out.println("user " + uIdx + " item " + iIdx + " val " +
//		// predictedValue);
//		if (Double.isNaN(predictedValue)) {
//			System.out.println("Prediction for user " + uIdx + " and item "
//					+ iIdx + " is NaN!");
//		}
//		return predictedValue;
    }
}