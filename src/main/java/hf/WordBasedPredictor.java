package hf;


import gnu.trove.iterator.*;
import gnu.trove.map.hash.*;
import gnu.trove.set.hash.TIntHashSet;
import onlab.core.Database;
import onlab.core.ExtendedDatabase;
import onlab.core.evaluation.Evaluation;
import onlab.core.predictor.Predictor;
import onlab.core.util.Util;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;

public class WordBasedPredictor extends Predictor {

    public static final String META_KEYS = "metaKeys";
    private static final String KEYVALUE_SEP = " ";
    private static final String KEYVALUE_WEIGHT_SEP = ":";
    private static final String KEY_VALUE_LIST_SEPARATOR = "[^\\p{L}0-9:_ ]";
    private static final String wordToWordSetsByKeyValueFileName = "wordToWordSetsByKeyValue";
    private static final String wordToWordSimilaritiesFileName = "wordToWordSimilarities";
    private static final String wordToWordSuppsFileName = "wordToWordSupps";
    private static final String wordSuppsByEventsFileName = "wordSuppsByEvents";
    private static final String wordSuppsByItemsFileName = "wordSuppsByItems.txt";
    private static final String wordsOfItemsSeenByUsersFileName = "wordsOfItemsSeenByUsers";
    private static final String wordIDsFileName = "wordIDs";
    private static final String reverseWordIDsFileName = "reverseWordIDs";

    //region Other member variables

    /**
     * Csak actor->actor, director->director, stb. hasonlosag???
     * (vagy minden mindennel: Actor->Director, Actor->VodMenu, stb)
     */
    private static final boolean compareOnlyMatchingKeyTypes = false;

    /**
     * A leghasonlobb N elem
     * compareOnlyMatchingKeyTypes = true alapján Actorhoz a leghasonlobb N actort veszi
     * compareOnlyMatchingKeyTypes = false esetén a leghasonlobb szo keytipusa nem szamit!
     */
    private static final int topN = 10;


    private static double LAMBDA = 0.0;

    public ExtendedDatabase dbExt = null;

    /**
     * key types (actor, director, etc.)
     */
    private String[] keys;

    /**
     * weight for each key type
     */
    private TCharDoubleHashMap weights;  //A-Actor, D-Director, V-VodMenu

    /**
     * Minden felhasznalora, hogy mely keyvalue-k jellemzok ra
     * <p/>
     * |----->A (Actor)     -->   ActorIDSet
     * |
     * uIdx-------->D (Director)  -->   DirectorIDSet
     * |
     * |----->V (VODMenu)   -->   VODMenuIDSet
     */
    private TIntObjectHashMap<TCharObjectHashMap<TIntHashSet>> wordsOfItemsSeenByUsers;

    /**
     * Az egyes metawordot mely metawordokkel neztek egyutt keyvalue tipusonkent
     */
    private TIntObjectHashMap<TCharObjectHashMap<TIntHashSet>> wordToWordSetsByKeyValue;

    /**
     * Mely metawordot mely metaworddel hanyszor fogyasztottak egyutt
     */
    private TObjectIntHashMap<Pair> wordToWordSupps;

    /**
     * Egymashoz 0-nal nagyobb hasonlosaggal rendelkezo metawordok
     */
    private TObjectDoubleHashMap<Pair> wordToWordSimilarities;

    /**
     * Az egyes keyvalue metawordok itemekben valo elofordulasa
     */
    public TObjectIntHashMap<String> wordSuppsByItems;

    /**
     * supp(metaWord) minden metaWord-re eventek alapjan
     */
    private TIntIntHashMap wordSuppsByEvents;

    /**
     * metaWord-hoz tartozo index
     */
    public TObjectIntHashMap<String> wordIDs;

    /**
     * indexhez tartozo metaWord
     */
    public TIntObjectHashMap<String> reverseWordIDs;

    /**
     * Predikcios folyamat nyomonkovetesere
     */
    private int testPercent;

    //endregion

    @Override
    public void train(Database db, Evaluation eval) {
        System.out.println("Started training...");
        this.db = db;

        dbExt = (ExtendedDatabase) db;

        testPercent = 1;

        String[] keys0 = par.get(META_KEYS).split(KEYVALUE_SEP);
        keys = new String[keys0.length];
        weights = new TCharDoubleHashMap(keys0.length);

        for (int i = 0; i < keys.length; i++) {
            String[] kv = keys0[i].split(KEYVALUE_WEIGHT_SEP);
            keys[i] = kv[0];
            weights.put(kv[0].charAt(0), Double.parseDouble(kv[1]));
        }


        try {
            if (!isFileExists("wordIDsTXT") || !isFileExists("reverseWordIDsTXT")) {
                indexKeyValues(true, false);
            } else {
                loadWordInfoFromFile("wordIDsBIN");
                loadWordInfoFromFile("reverseWordIDsBIN");
            }

            if (!isFileExists("wordToWordSimilaritiesTXT")) {
                if (!isFileExists("wordToWordSuppsTXT") || !isFileExists("wordSuppsByEventsBIN") || !isFileExists("wordsOfItemsSeenByUsersBIN") || !isFileExists("wordToWordSetsByKeyValueBIN")) {
                    countWordSupps(true, true, true, true);
                } else {
                    loadWordInfoFromFile("wordSuppsByEventsBIN");
                    loadWordInfoFromFile("wordToWordSuppsTXT");
                }
                computeWordSims(true);
                loadWordInfoFromFile("wordsOfItemsSeenByUsersBIN");
            } else {
                loadWordInfoFromFile("wordsOfItemsSeenByUsersBIN");
                loadWordInfoFromFile("wordToWordSimilaritiesTXT");
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }

//        listTopNWordToWord(2888,false);
//        listTopNWordToWord(2888,true);
//        listTopNWordToWord(94,true);
//        listTopNWordToWord(94,false);
        listTopNWordToWord(807,true);
//        listTopNWordToWord(807,false);
//        listTopNWordToWord(793,true);
//        listTopNWordToWord(2795,true);
//        listTopNWordToWord(2795,false);
    }


    /**
     * metaWord indexelo: (metaWord) --> int ID
     *
     * @param countMetaWordsByItem Szamoljon-e supp(mW)-öt itemek alapjan?
     */
    public void indexKeyValues(boolean printIndices, boolean countMetaWordsByItem) {
        System.out.println("metaWordok indexelese...");
        wordIDs = new TObjectIntHashMap<String>(10500);
        reverseWordIDs = new TIntObjectHashMap<String>(10500);
        if (countMetaWordsByItem) wordSuppsByItems = new TObjectIntHashMap<String>(10500);

        int seq = 0;    //metaWord ID
        for (Database.Item i : db.items(null)) {
            for (String key : keys) {
                String keyAll = dbExt.getItemKeyValue(i.idx, key);
                String[] values = keyAll.split(KEY_VALUE_LIST_SEPARATOR); // /f és / jel menten felbontva
                char keyType = key.charAt(0);
                for (String val : values) {
                    if (val.equals(""))
                        continue;
                    String mWrd = keyType + ":" + val.toLowerCase();
                    if (!wordIDs.containsKey(mWrd)) {
                        seq++;
                        wordIDs.put(mWrd, seq);
                        reverseWordIDs.put(seq, mWrd);
                    }
                    if (countMetaWordsByItem) {
                        wordSuppsByItems.put(mWrd, wordSuppsByItems.get(mWrd) + 1);
                    }
                }
            }
        }
        if (printIndices) printSuppsToFile("wordIDsBIN");
        if (printIndices) printSuppsToFile("wordIDsTXT");
        if (printIndices) printSuppsToFile("reverseWordIDsBIN");
        if (printIndices) printSuppsToFile("reverseWordIDsTXT");
        if (countMetaWordsByItem) printSuppsToFile("wordSuppsByItems");
    }

    public void countWordSupps(boolean printWordSuppsByEventsToFile, boolean printwordToWordSuppsToFile, boolean printWordsOfItemsSeenByUsersToFile, boolean printwordToWordSetsByKeyValueToFile) {
        System.out.println("wordSupps, wordToWordSupps es wordsOfItemsSeenByUsers szamitasa...");
        int nEvents = 0;
        int percent = 1;
        int size = db.numEvents();
        int percent1 = size / 100;
        TIntObjectHashMap<TCharObjectHashMap<TIntHashSet>> wordSetByKeyTypesOfAllItems = new TIntObjectHashMap<TCharObjectHashMap<TIntHashSet>>(db.numItems());
        wordsOfItemsSeenByUsers = new TIntObjectHashMap<TCharObjectHashMap<TIntHashSet>>();
        wordToWordSetsByKeyValue = new TIntObjectHashMap<TCharObjectHashMap<TIntHashSet>>();
        wordToWordSupps = new TObjectIntHashMap<Pair>();
        wordSuppsByEvents = new TIntIntHashMap();
        for (Database.Event e : db.events(null)) {
            nEvents++;
            int iIdx = e.iIdx;
            int uIdx = e.uIdx;

            TCharObjectHashMap<TIntHashSet> appearedWordsInThisItem;
            appearedWordsInThisItem = getWordMapOfItem(wordSetByKeyTypesOfAllItems, iIdx);

            TCharObjectHashMap<TIntHashSet> allWordsOfActualUser;
            allWordsOfActualUser = getWordMapOfUser(uIdx);

            TCharObjectIterator<TIntHashSet> iterator = appearedWordsInThisItem.iterator();
            while (iterator.hasNext()) {
                iterator.advance();
                char keyType = iterator.key();
                TIntHashSet appearedWordsInThisItemByKeyType = iterator.value();

                TIntIterator iterator1 = appearedWordsInThisItemByKeyType.iterator();
                while (iterator1.hasNext()) {
                    int wordID = iterator1.next();
                    if (wordID == 0) System.out.println("Elvileg nem letezo metawordot talalt!");

                    wordSuppsByEvents.put(wordID, wordSuppsByEvents.get(wordID) + 1);       //supp(metaWord) szamitashoz

                    boolean actualUserMapContainsWord = allWordsOfActualUser.get(keyType).contains(wordID);
                    TCharObjectIterator<TIntHashSet> iterator2 = allWordsOfActualUser.iterator();
                    while (iterator2.hasNext()) {
                        iterator2.advance();
                        char _wordType = iterator2.key();
                        TIntHashSet wordsByKeyTypeOfActualUser = iterator2.value();

                        TIntIterator iterator3 = wordsByKeyTypeOfActualUser.iterator();
                        while (iterator3.hasNext()) {
                            int _wordID = iterator3.next();
                            if (!actualUserMapContainsWord) {
                                linkWords(_wordID, _wordType, wordID, keyType);
                            }
                            increaseWordPairSupp(_wordID, wordID);
                        }
                    }
                    if (!actualUserMapContainsWord)
                        allWordsOfActualUser.get(keyType).add(wordID);
                }
            }
            if (nEvents == percent1) {
                System.out.print(percent + "% ");
                percent++;
                nEvents = 0;
            }
        }
        if (printWordSuppsByEventsToFile) {
            printSuppsToFile("wordSuppsByEventsTXT");
            printSuppsToFile("wordSuppsByEventsBIN");
        }
        if (printwordToWordSuppsToFile) {
            printSuppsToFile("wordToWordSuppsTXT");
        }
        if (printWordsOfItemsSeenByUsersToFile) {
            printSuppsToFile("wordsOfItemsSeenByUsersBIN");
        }
        if (printwordToWordSetsByKeyValueToFile) {
            printSuppsToFile("wordToWordSetsByKeyValueBIN");
        }
    }

    private TCharObjectHashMap<TIntHashSet> getWordMapOfItem(TIntObjectHashMap<TCharObjectHashMap<TIntHashSet>> wordSetByKeyTypesOfAllItems, int iIdx) {
        TCharObjectHashMap<TIntHashSet> appearedWordsInThisItem;
        if (!wordSetByKeyTypesOfAllItems.contains(iIdx)) {
            appearedWordsInThisItem = getItemMetaWordSet(iIdx);
            wordSetByKeyTypesOfAllItems.put(iIdx, appearedWordsInThisItem);
        } else {
            appearedWordsInThisItem = wordSetByKeyTypesOfAllItems.get(iIdx);
        }
        return appearedWordsInThisItem;
    }

    private TCharObjectHashMap<TIntHashSet> getWordMapOfUser(int uIdx) {
        TCharObjectHashMap<TIntHashSet> allWordsOfActualUser;
        if (!wordsOfItemsSeenByUsers.containsKey(uIdx)) {
            allWordsOfActualUser = initNewWordMapForItem();
            wordsOfItemsSeenByUsers.put(uIdx, allWordsOfActualUser);
        } else {
            allWordsOfActualUser = wordsOfItemsSeenByUsers.get(uIdx);
        }
        return allWordsOfActualUser;
    }

    private TCharObjectHashMap<TIntHashSet> initNewWordMapForItem() {
        TCharObjectHashMap<TIntHashSet> wordMapForItem = new TCharObjectHashMap<TIntHashSet>(keys.length);
        for (String s : keys) {
            wordMapForItem.put(s.charAt(0), new TIntHashSet());
        }
        return wordMapForItem;
    }

    private TCharObjectHashMap<TIntHashSet> getItemMetaWordSet(int iIdx) {
        TCharObjectHashMap<TIntHashSet> appearedWordsInThisItem = new TCharObjectHashMap<TIntHashSet>(keys.length);
        for (String key : keys) {
            String keyAll = dbExt.getItemKeyValue(iIdx, key);
            String[] values = keyAll.split(KEY_VALUE_LIST_SEPARATOR); // /f és / jel menten felbontva
            char keyType = key.charAt(0);
            TIntHashSet wordSetByKeyType = new TIntHashSet();
            for (String val : values) {
                if (val.equals(""))
                    continue;
                String mWrd = keyType + ":" + val.toLowerCase();
                wordSetByKeyType.add(wordIDs.get(mWrd));
            }
            appearedWordsInThisItem.put(keyType, wordSetByKeyType);
        }
        return appearedWordsInThisItem;
    }

    private void increaseWordPairSupp(int word1, int word2) {
        if (word1 == word2) return;
        Pair pair = new Pair(word1, word2);
        wordToWordSupps.put(pair, wordToWordSupps.get(pair) + 1);
    }

    private void linkWords(int word1, char keyType1, int word2, char keyType2) {
        if (word1 == word2) return;
        getLinkedWords(word1, keyType2).add(word2);
        getLinkedWords(word2, keyType1).add(word1);
    }

    private TIntHashSet getLinkedWords(int item, char keyType) {
        if (wordToWordSetsByKeyValue.contains(item)) {
            return wordToWordSetsByKeyValue.get(item).get(keyType);
        } else {
            //ha ez az elem nincs meg a mapben, letrehozom ot ures intsettel minden keytype-hoz
            TCharObjectHashMap<TIntHashSet> linkedItems = new TCharObjectHashMap<TIntHashSet>();
            for (String key : keys) {
                char _keyType = key.charAt(0);
                TIntHashSet hs = new TIntHashSet();
                linkedItems.put(_keyType, hs);
            }
            wordToWordSetsByKeyValue.put(item, linkedItems);
            return linkedItems.get(keyType);        //visszaadom a boviteni kivant keytpye setjet!
        }
    }

    public void computeWordSims(boolean printSimsToFile) {
        System.out.println("wordTowordSimilarity szamitas!");
        int nPairs = 0;
        int percent = 1;
        int size = wordToWordSupps.size();
        int percent1 = size / 100;
        wordToWordSimilarities = new TObjectDoubleHashMap<Pair>(size);
        TObjectIntIterator<Pair> iterator = wordToWordSupps.iterator();
        while (iterator.hasNext()) {
            nPairs++;
            iterator.advance();
            Pair p = iterator.key();
            int suppAB = iterator.value();
            int suppA = wordSuppsByEvents.get(p.iIdx1);
            int suppB = wordSuppsByEvents.get(p.iIdx2);

            double sim;
            sim = (double) suppAB
                    / (Math.sqrt(suppA + LAMBDA) * Math
                    .sqrt(suppB + LAMBDA));
            wordToWordSimilarities.put(p, sim);

            if (nPairs == percent1) {
                System.out.print(percent + "% ");
                percent++;
                nPairs = 0;
            }
        }


        if (printSimsToFile) {
            printSuppsToFile("wordToWordSimilaritiesTXT");
            printSuppsToFile("wordToWordSimilaritiesBIN");
        }
        System.out.println("wordToWordSimilarities szamitas KESZ");
    }

    public void listTopNWordToWord(int wordID, boolean selectOnlySameKeyTypeWords) {
        if (wordToWordSetsByKeyValue == null) {
            try {
                loadWordInfoFromFile("wordToWordSetsByKeyValueBIN");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        //TODO Hogy fordulhat elo Nincs ilyen Word hibauzi?
        TCharObjectHashMap<TIntHashSet> wordMapByKeyTypesForWord = wordToWordSetsByKeyValue.get(wordID);
        if(wordMapByKeyTypesForWord == null){
            System.out.println("Nincs ilyen word!");
            return;
        }
        TCharObjectIterator<TIntHashSet> iterator = wordMapByKeyTypesForWord.iterator();
        if (!selectOnlySameKeyTypeWords) {
            //a szohoz tartozo barmilyen keyType-u leghasonlobb N word
            ArrayList<ObjectSim> list = new ArrayList<ObjectSim>();
            while (iterator.hasNext()) {
                iterator.advance();
                TIntHashSet hs = wordMapByKeyTypesForWord.get(iterator.key());

                addWordSimilaritiesToList(wordID, list, hs);
            }
            Collections.sort(list);
            int numOfSims = list.size();
            int i = topN > numOfSims ? numOfSims : topN;
            int j = 0;
            System.out.println(reverseWordIDs.get(wordID) + " leghasonlobb " + i + " tipusfuggetlen szava:");
            while (i-- > 0) {
                ObjectSim o = list.get(j);
                System.out.println(j + ".:\t" + reverseWordIDs.get(o.idx) + "\t" + o.sim);
                j++;
            }
        } else {
            //a szohoz tartozo azonos keyType-u leghasonlobb N word
            TCharObjectHashMap<ArrayList<ObjectSim>> listsByKeyTypes = new TCharObjectHashMap<ArrayList<ObjectSim>>(keys.length);
            while (iterator.hasNext()) {
                iterator.advance();
                TIntHashSet hs = wordMapByKeyTypesForWord.get(iterator.key());

                ArrayList<ObjectSim> listByKeyType = new ArrayList<ObjectSim>();
                addWordSimilaritiesToList(wordID, listByKeyType, hs);
                listsByKeyTypes.put(iterator.key(), listByKeyType);
            }

            TCharObjectIterator<ArrayList<ObjectSim>> iterator2 = listsByKeyTypes.iterator();
            while (iterator2.hasNext()) {
                iterator2.advance();
                Collections.sort(iterator2.value());
            }

            iterator2 = listsByKeyTypes.iterator();
            while (iterator2.hasNext()) {
                iterator2.advance();
                ArrayList<ObjectSim> listByKeyType = iterator2.value();
                int numOfSims = listByKeyType.size();
                int i = topN > numOfSims ? numOfSims : topN;
                int j = 0;
                System.out.println(reverseWordIDs.get(wordID) + " leghasonlobb " + i + " " + iterator2.key() + " szava:");
                while (i-- > 0) {
                    ObjectSim o = listByKeyType.get(j);
                    System.out.println(j + ".:\t" + reverseWordIDs.get(o.idx) + "\t" + o.sim);
                    j++;
                }
            }
        }
    }

    private void addWordSimilaritiesToList(int wordID, ArrayList<ObjectSim> list, TIntHashSet hs) {
        TIntIterator iterator2 = hs.iterator();
        while (iterator2.hasNext()) {
            int _wordID = iterator2.next();
            double similarity = wordToWordSimilarities.get(new Pair(wordID, _wordID));
            list.add(new ObjectSim(_wordID, similarity));
        }
    }


    public void printSuppsToFile(String type) {
        PrintWriter out = null;
        if (type.equals("wordSuppsByEventsTXT")) {
            try {
                System.out.println("Printing support of metawords by events...");
                out = new PrintWriter(wordSuppsByEventsFileName + ".txt");
                TIntIntIterator iterator = wordSuppsByEvents.iterator();
                while (iterator.hasNext()) {
                    iterator.advance();
                    out.println(reverseWordIDs.get(iterator.key()) + ":" + iterator.value());
                }
                System.out.println("Printing metaword supps by events is successful!");
            } catch (Exception e) {
                System.out.println("Printing metaword supps by events is unsuccessful:");
                System.err.println(e.getMessage());
            } finally {
                if (out != null) {
                    out.close();
                }
            }
        } else if (type.equals("wordSuppsByEventsBIN")) {
            System.out.println("wordSuppsByEvents mentese BINbe");
            try {
                saveObject(wordSuppsByEvents, wordSuppsByEventsFileName);
                System.out.println("wordSuppsByEvents mentese sikeres!");
            } catch (IOException e) {
                System.out.println("wordSuppsByEvents mentese sikertelen:");
                e.printStackTrace();
            }
        } else if (type.equals("wordToWordSimilaritiesTXT")) {
            try {
                System.out.println("Printing wordToWordSimilarities to TXT...");
                out = new PrintWriter(wordToWordSimilaritiesFileName + ".txt");
                TObjectDoubleIterator<Pair> iterator = wordToWordSimilarities.iterator();
                while (iterator.hasNext()) {
                    iterator.advance();
                    Pair p = iterator.key();
                    out.println(p.iIdx1 + "\t" + p.iIdx2 + "\t" + iterator.value());
                }
                System.out.println("Printing metaword supps by events is successful!");
            } catch (Exception e) {
                System.out.println("Printing metaword supps by events is unsuccessful:");
                System.err.println(e.getMessage());
            } finally {
                if (out != null) {
                    out.close();
                }
            }
        } else if (type.equals("wordToWordSimilaritiesBIN")) {
            System.out.println("wordToWordSimilarities mentese BINbe");
            try {
                saveObject(wordToWordSimilarities, wordToWordSimilaritiesFileName);
                System.out.println("wordToWordSimilarities mentese sikeres!");
            } catch (IOException e) {
                System.out.println("wordToWordSimilarities mentese sikertelen:");
                e.printStackTrace();
            }
        } else if (type.equals("wordToWordSetsByKeyValueBIN")) {
            System.out.println("wordToWordSetsByKeyValue mentese BINbe");
            try {
                saveObject(wordToWordSetsByKeyValue, wordToWordSetsByKeyValueFileName);
                System.out.println("wordToWordSetsByKeyValueBIN mentese sikeres!");
            } catch (IOException e) {
                System.out.println("wordToWordSetsByKeyValueBIN mentese sikertelen:");
                e.printStackTrace();
            }
        } else if (type.equals("wordSuppsByItems")) {
            try {
                System.out.println("Printing support of metawords...");
                out = new PrintWriter(wordSuppsByItemsFileName);
                TObjectIntIterator<String> iterator = wordSuppsByItems.iterator();
                while (iterator.hasNext()) {
                    iterator.advance();
                    out.println(iterator.key() + ":" + iterator.value());
                }
                System.out.println("Printing metaword supps by items is successful!");
            } catch (Exception e) {
                System.out.println("Printing metaword supps by items is unsuccessful:");
                System.err.println(e.getMessage());
            } finally {
                if (out != null) {
                    out.close();
                }
            }
        } else if (type.equals("wordToWordSuppsTXT")) {
            try {
                System.out.println("Printing support of wordpairs into file " + wordToWordSuppsFileName + ".txt");
                out = new PrintWriter(wordToWordSuppsFileName + ".txt");
                TObjectIntIterator<Pair> iterator = wordToWordSupps.iterator();
                while (iterator.hasNext()) {
                    iterator.advance();
                    Pair p = iterator.key();
                    out.println(p.iIdx1 + "\t" + p.iIdx2 + "\t" + iterator.value());
                }
                System.out.println("Printing itempairsupps is successful!");
            } catch (Exception e) {
                System.out.println("Printing itempairsupps is unsuccessful!");
                System.err.println(e.getMessage());
            } finally {
                if (out != null) {
                    out.close();
                }
            }
        } else if (type.equals("wordToWordSuppsBIN")) {
            System.out.println("wordToWordSuppsTXT mentese BINbe");
            try {
                saveObject(wordToWordSupps, wordToWordSuppsFileName);
                System.out.println("wordToWordSuppsTXT mentese sikeres!");
            } catch (IOException e) {
                System.out.println("wordToWordSuppsTXT mentese sikertelen:");
                e.printStackTrace();
            }
        } else if (type.equals("wordIDsTXT")) {
            try {
                System.out.println("Printing indices of metawords to txt...");
                out = new PrintWriter(wordIDsFileName + ".txt");
                TObjectIntIterator<String> iterator = wordIDs.iterator();
                while (iterator.hasNext()) {
                    iterator.advance();
                    out.println(iterator.key() + ":" + iterator.value());
                }
                System.out.println("Printing metaword indices is successful!");
            } catch (Exception e) {
                System.out.println("Printing metaword indices is unsuccessful:");
                System.err.println(e.getMessage());
            } finally {
                if (out != null) {
                    out.close();
                }
            }
        } else if (type.equals("wordIDsBIN")) {
            System.out.println("wordIDs mentese BINbe");
            try {
                saveObject(wordIDs, wordIDsFileName);
                System.out.println("wordIDs mentese sikeres!");
            } catch (IOException e) {
                System.out.println("wordIDs mentese sikertelen:");
                e.printStackTrace();
            }
        } else if (type.equals("reverseWordIDsTXT")) {
            try {
                System.out.println("Printing reverse indices of metawords to txt...");
                out = new PrintWriter(reverseWordIDsFileName + ".txt");
                TIntObjectIterator<String> iterator = reverseWordIDs.iterator();
                while (iterator.hasNext()) {
                    iterator.advance();
                    out.println(iterator.key() + ":" + iterator.value());
                }
                System.out.println("Printing metaword reverse indices is successful!");
            } catch (Exception e) {
                System.out.println("Printing metaword reverse indices is unsuccessful:");
                System.err.println(e.getMessage());
            } finally {
                if (out != null) {
                    out.close();
                }
            }
        } else if (type.equals("reverseWordIDsBIN")) {
            System.out.println("reverseWordIDs mentese BINbe");
            try {
                saveObject(reverseWordIDs, reverseWordIDsFileName);
                System.out.println("reverseWordIDs mentese sikeres!");
            } catch (IOException e) {
                System.out.println("reverseWordIDs mentese sikertelen:");
                e.printStackTrace();
            }
        } else if (type.equals("wordsOfItemsSeenByUsersBIN")) {
            System.out.println("wordsOfItemsSeenByUsers mentese BINbe");
            try {
                saveObject(wordsOfItemsSeenByUsers, wordsOfItemsSeenByUsersFileName);
                System.out.println("wordsOfItemsSeenByUsersBIN mentese sikeres!");
            } catch (IOException e) {
                System.out.println("wordsOfItemsSeenByUsersBIN mentese sikertelen:");
                e.printStackTrace();
            }
        } else try {
            throw new Exception("Nincs ilyen betoltesi parancs!");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void saveObject(Serializable object, String path) throws IOException {
        ObjectOutputStream out = Util.myObjectOutputStream(path);
        out.writeObject(object);
        out.close();
    }

    public static Object loadObject(String path) throws IOException, ClassNotFoundException {
        ObjectInputStream in = Util.myObjectInputStream(path);
        Object object = in.readObject();
        in.close();
        return object;
    }

    public boolean isFileExists(String filetype) throws FileNotFoundException {
        if (filetype.equals("wordIDsBIN")) {
            File f = new File(wordIDsFileName);
            return f.exists() && f.isFile();
        } else if (filetype.equals("wordIDsTXT")) {
            File f = new File(wordIDsFileName + ".txt");
            return f.exists() && f.isFile();
        } else if (filetype.equals("reverseWordIDsBIN")) {
            File f = new File(reverseWordIDsFileName);
            return f.exists() && f.isFile();
        } else if (filetype.equals("reverseWordIDsTXT")) {
            File f = new File(reverseWordIDsFileName + ".txt");
            return f.exists() && f.isFile();
        } else if (filetype.equals("wordToWordSimilaritiesBIN")) {
            File f = new File(wordToWordSimilaritiesFileName);
            return f.exists() && f.isFile();
        } else if (filetype.equals("wordToWordSimilaritiesTXT")) {
            File f = new File(wordToWordSimilaritiesFileName + ".txt");
            return f.exists() && f.isFile();
        } else if (filetype.equals("wordToWordSuppsTXT")) {
            File f = new File(wordToWordSuppsFileName + ".txt");
            return f.exists() && f.isFile();
        } else if (filetype.equals("wordToWordSuppsBIN")) {
            File f = new File(wordToWordSuppsFileName);
            return f.exists() && f.isFile();
        } else if (filetype.equals("wordSuppsByEventsTXT")) {
            File f = new File(wordSuppsByEventsFileName + ".txt");
            return f.exists() && f.isFile();
        } else if (filetype.equals("wordSuppsByEventsBIN")) {
            File f = new File(wordSuppsByEventsFileName);
            return f.exists() && f.isFile();
        } else if (filetype.equals("wordSuppsByItemsTXT")) {
            File f = new File(wordSuppsByItemsFileName);
            return f.exists() && f.isFile();
        } else if (filetype.equals("wordsOfItemsSeenByUsers")) {
            File f = new File(wordsOfItemsSeenByUsersFileName);
            return f.exists() && f.isFile();
        } else if (filetype.equals("wordToWordSetsByKeyValueBIN")) {
            File f = new File(wordToWordSetsByKeyValueFileName);
            return f.exists() && f.isFile();
        } else throw new FileNotFoundException("Hibas fajltipus: " + filetype);
    }

    public boolean loadWordInfoFromFile(String itemInfoType) throws Exception {
        BufferedReader reader = null;
        if (itemInfoType.equals("wordSuppsByEventsTXT")) {
            System.out.println("Loading word Supports...");
            wordSuppsByEvents = new TIntIntHashMap(14000);
            try {
                reader = new BufferedReader(new FileReader(wordSuppsByEventsFileName + ".txt"));
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] splittedLine = line.split("\t");
                    wordSuppsByEvents.put(Integer.parseInt(splittedLine[0]), Integer.parseInt(splittedLine[1]));
                }
                System.out.println("Loading word Supports done!");
                return true;
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("Loading word Supports failed!");
                return false;
            } finally {
                if (reader != null) {
                    reader.close();
                }
            }
        } else if (itemInfoType.equals("wordSuppsByEventsBIN")) {
            System.out.println("wordSuppsByEvents betoltese");
            try {
                wordSuppsByEvents = (TIntIntHashMap) loadObject(wordSuppsByEventsFileName);
                System.out.println("wordSuppsByEvents betoltese sikeres!");
                return true;
            } catch (IOException e) {
                System.out.println("wordSuppsByEvents betoltese sikertelen:");
                e.printStackTrace();
                return false;
            } catch (ClassNotFoundException e) {
                System.out.println("wordSuppsByEvents betoltese sikertelen:");
                e.printStackTrace();
                return false;
            }
        } else if (itemInfoType.equals("wordToWordSetsByKeyValueBIN")) {
            System.out.println("wordToWordSetsByKeyValue betoltese");
            try {
                wordToWordSetsByKeyValue = (TIntObjectHashMap<TCharObjectHashMap<TIntHashSet>>) loadObject(wordToWordSetsByKeyValueFileName);
                System.out.println("wordToWordSetsByKeyValue betoltese sikeres!");
                return true;
            } catch (IOException e) {
                System.out.println("wordToWordSetsByKeyValue betoltese sikertelen:");
                e.printStackTrace();
                return false;
            } catch (ClassNotFoundException e) {
                System.out.println("wordToWordSetsByKeyValue betoltese sikertelen:");
                e.printStackTrace();
                return false;
            }
        } else if (itemInfoType.equals("wordToWordSuppsTXT")) {
            System.out.println("Loading item Pair Supports...");
            int importedRows = 0;
            int percent = 1;
            wordToWordSupps = new TObjectIntHashMap(20000000);
            try {
                reader = new BufferedReader(new FileReader(wordToWordSuppsFileName + ".txt"));
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] splittedLine = line.split("\t");
                    wordToWordSupps.put(new Pair(Integer.parseInt(splittedLine[0]), Integer.parseInt(splittedLine[1])), Integer.parseInt(splittedLine[2]));
                    importedRows++;
                    if (importedRows == 195550) {
                        System.out.print(percent + "%  ");
                        importedRows = 0;
                        percent++;
                    }
                }
                System.out.println("Loading item Pair Supports done!");
                return true;
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("Loading item Pair Supports failed!");
                return false;
            } finally {
                if (reader != null) {
                    reader.close();
                }
            }
        } else if (itemInfoType.equals("wordToWordSimilaritiesTXT")) {
            System.out.println("Loading wordToWordSimilarities...");
            int importedRows = 0;
            int percent = 1;
            wordToWordSimilarities = new TObjectDoubleHashMap(20500000);
            try {
                reader = new BufferedReader(new FileReader(wordToWordSimilaritiesFileName + ".txt"));
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] splittedLine = line.split("\t");
                    wordToWordSimilarities.put(new Pair(Integer.parseInt(splittedLine[0]), Integer.parseInt(splittedLine[1])), Double.parseDouble(splittedLine[2]));
                    importedRows++;
                    if (importedRows == 195550) {
                        System.out.print(percent + "%  ");
                        importedRows = 0;
                        percent++;
                    }
                }
                System.out.println("Loading wordToWordSimilarities  done!");
                return true;
            } catch (Exception e) {
                System.out.println("Loading wordToWordSimilarities failed!");
                return false;
            } finally {
                if (reader != null) {
                    reader.close();
                }
            }
        } else if (itemInfoType.equals("wordToWordSimilaritiesBIN")) {
            System.out.println("wordToWordSimilarities betoltese");
            try {
                wordToWordSimilarities = (TObjectDoubleHashMap<Pair>) loadObject(wordToWordSimilaritiesFileName);
                System.out.println("wordToWordSimilarities betoltese sikeres!");
                return true;
            } catch (IOException e) {
                System.out.println("wordToWordSimilarities betoltese sikertelen:");
                e.printStackTrace();
                return false;
            } catch (ClassNotFoundException e) {
                System.out.println("wordToWordSimilarities betoltese sikertelen:");
                e.printStackTrace();
                return false;
            }
        } else if (itemInfoType.equals("wordsOfItemsSeenByUsersBIN")) {
            System.out.println("wordsOfItemsSeenByUsers betoltese");
            try {
                wordsOfItemsSeenByUsers = (TIntObjectHashMap<TCharObjectHashMap<TIntHashSet>>) loadObject(wordsOfItemsSeenByUsersFileName);
                System.out.println("wordsOfItemsSeenByUsers betoltese sikeres!");
                return true;
            } catch (IOException e) {
                System.out.println("wordsOfItemsSeenByUsers betoltese sikertelen:");
                e.printStackTrace();
                return false;
            } catch (ClassNotFoundException e) {
                System.out.println("wordsOfItemsSeenByUsers betoltese sikertelen:");
                e.printStackTrace();
                return false;
            }
        } else if (itemInfoType.equals("wordIDsBIN")) {
            System.out.println("wordIDsBIN betoltese");
            try {
                wordIDs = (TObjectIntHashMap) loadObject(wordIDsFileName);
                System.out.println("wordIDs betoltese sikeres!");
                return true;
            } catch (IOException e) {
                System.out.println("wordIDs betoltese sikertelen:");
                e.printStackTrace();
                return false;
            } catch (ClassNotFoundException e) {
                System.out.println("wordIDs betoltese sikertelen:");
                e.printStackTrace();
                return false;
            }
        } else if (itemInfoType.equals("reverseWordIDsBIN")) {
            System.out.println("reverseWordIDsBIN betoltese");
            try {
                reverseWordIDs = (TIntObjectHashMap) loadObject(reverseWordIDsFileName);
                System.out.println("reverseWordIDs betoltese sikeres!");
                return true;
            } catch (IOException e) {
                System.out.println("reverseWordIDs betoltese sikertelen:");
                e.printStackTrace();
                return false;
            } catch (ClassNotFoundException e) {
                System.out.println("reverseWordIDs betoltese sikertelen:");
                e.printStackTrace();
                return false;
            }
        } else throw new Exception("Nincs ilyen betoltesi parancs!");
    }


    TCharObjectHashMap<TIntHashSet> wordsOfItemsSeenByLastUser;
    int lastUser = -1;
    int numOfUsers = 0;

    public double predict(int uIdx, int iIdx, long time) {
        double prediction = 0.0;

        if (lastUser != uIdx) {
            TCharObjectHashMap<TIntHashSet> wordsOfItemsSeenByActualUser = wordsOfItemsSeenByUsers.get(uIdx);
            if (wordsOfItemsSeenByActualUser != null) {
                wordsOfItemsSeenByLastUser = wordsOfItemsSeenByActualUser;
            } else {
                wordsOfItemsSeenByLastUser = null;
            }
            lastUser = uIdx;
            numOfUsers++;
        }

        if (wordsOfItemsSeenByLastUser != null) {

            TCharObjectHashMap<TIntHashSet> appearedWordsInThisItem = getItemMetaWordSet(iIdx);
            TCharObjectIterator<TIntHashSet> iterator = appearedWordsInThisItem.iterator();

            while (iterator.hasNext()) {
                iterator.advance();
                char keyType = iterator.key();
                TIntHashSet appearedWordsInThisItemByKeyType = iterator.value();

                TIntIterator iterator1 = appearedWordsInThisItemByKeyType.iterator();
                while (iterator1.hasNext()) {
                    int wordID = iterator1.next();     //metaWordhoz tartozo index

                    if (!compareOnlyMatchingKeyTypes) {
                        //adott szot keytype-tol fuggetlenul a User minden szavahoz hasonlitunk
                        //így pl. az Actor - VODMenu word hasonlosag is beleszamit
                        TCharObjectIterator<TIntHashSet> iterator2 = wordsOfItemsSeenByLastUser.iterator();
                        while (iterator2.hasNext()) {
                            iterator2.advance();
                            char _wordType = iterator2.key();
                            TIntHashSet wordsByKeyTypeOfUser = iterator2.value();

                            TIntIterator iterator3 = wordsByKeyTypeOfUser.iterator();
                            while (iterator3.hasNext()) {
                                int _wordID = iterator3.next();
                                double similarity = wordID == _wordID ? 1.0 : wordToWordSimilarities.get(new Pair(wordID, _wordID));
                                prediction += similarity;
                            }
                        }
                    } else {
                        //adott szot keytype-tol fuggoen csak a User azonos keytype-u szavaihoz hasonlitunk
                        TIntIterator iterator2 = wordsOfItemsSeenByLastUser.get(keyType).iterator();
                        while (iterator2.hasNext()) {
                            int _wordID = iterator2.next();
                            double similarity = wordID == _wordID ? 1.0 : wordToWordSimilarities.get(new Pair(wordID, _wordID));
                            prediction += similarity;
                        }
                    }
                }
            }
        }
        if (uIdx / 10000 > testPercent) {
            System.out.print(testPercent + " ");
            testPercent++;
        }
        return prediction;
    }
}
