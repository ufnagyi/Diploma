package hf.JavaPredictors;

import gnu.trove.iterator.TObjectDoubleIterator;
import gnu.trove.iterator.TObjectIntIterator;
import gnu.trove.map.hash.*;
import onlab.core.Database;
import onlab.core.ExtendedDatabase;
import onlab.core.evaluation.Evaluation;
import onlab.core.predictor.Predictor;
import onlab.core.util.Util;

import java.io.*;
import java.util.HashSet;

public class ContentBasedPredictor extends Predictor {

    public static final String META_KEYS = "metaKeys";
    private static final String KEYVALUE_SEP = " ";
    private static final String KEYVALUE_WEIGHT_SEP = ":";
    private static final String KEY_VALUE_LIST_SEPARATOR = "[^\\p{L}0-9:_. ]";
    private static final String wordPairSuppsFileName = "wordPairSupps.txt";
    private static final String metaWordSuppsByItemFileName = "metaWordSuppsByItem.txt";
    private static final String metaWordSuppsByEventFileName = "metaWordSuppsByEvent.txt";
    private static final String metaWordIndicesFileName = "metaWordIndices.txt";

    public ExtendedDatabase dbExt = null;
    private String[] keys;
    private TCharDoubleHashMap weights;  //A-Actor, D-Director, V-VodMenu

    private TIntObjectHashMap<TObjectDoubleHashMap<String>> userProfiles;

    /**
     * Az egyes keyvalue metawordok itemekben valo elofordulasa
     */
    public TObjectIntHashMap<String> metaWordSuppsByItems;


    /**
     * Az egyes keyvalue metawordok elofordulasa Eventek alapjan
     */
    public TObjectIntHashMap<String> metaWordSuppsByEvents;

    /**
     * index minden metaWord-re
     */
    public TObjectIntHashMap<String> metaWords;

    private int testPercent;

    @Override
    protected void initParameters() {
        par.put(META_KEYS, "");
    }



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
        if(!loadExistingUserProfiles()) {
            createAllUserMetaWordProfiles(true);
        }
        testPercent = 1;
    }



    /**
     * metaWord indexelő
     *
     * @param countMetaWordsByItem Szamoljon-e supp(mW)-öt itemek alapjan?
     */
    public void indexKeyValues(boolean printIndices, boolean countMetaWordsByItem) {
        metaWords = new TObjectIntHashMap<>(10500);
        if (countMetaWordsByItem) metaWordSuppsByItems = new TObjectIntHashMap<>(10500);

        int seq = 0;
        for (Database.Item i : db.items(null)) {
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
                        metaWordSuppsByItems.put(mWrd, metaWordSuppsByItems.get(mWrd) + 1);
                    }
                }
            }
        }
        if (printIndices) printSuppsToFile("metaWordIndices");
        if (countMetaWordsByItem) printSuppsToFile("wordSuppsByItems");
    }




    public boolean loadExistingUserProfiles(){
        System.out.println("UserProfiles betoltese");
        try {
            userProfiles = (TIntObjectHashMap<TObjectDoubleHashMap<String>>) loadObject("userMetaWordProfiles");
            System.out.println("UserProfile betoltese sikeres!");
            return true;
        } catch (IOException e) {
            System.out.println("UserProfile betoltese sikertelen:");
            e.printStackTrace();
            return false;
        } catch (ClassNotFoundException e) {
            System.out.println("UserProfile betoltese sikertelen:");
            e.printStackTrace();
            return false;
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


    /**
     * kapott user metaword előfordulásai alapján számolja azok súlyát
     */
    public void createAllUserMetaWordProfiles(boolean saveProfilesToFile) {
        userProfiles = new TIntObjectHashMap<>(dbExt.numUsers());
        int nUsers = 0;
        int percent = 1;
        int size = db.numUsers();
        int percent1 = size / 100;
        for (Database.User u : dbExt.users(null)) {
            nUsers++;
            userProfiles.put(u.idx,createUserMetaWordProfile(u.idx));
            if (nUsers == percent1) {
                System.out.print(percent + "% ");
                percent++;
                nUsers = 0;
            }
        }
        if(saveProfilesToFile) {
            System.out.println("UserProfiles mentese");
            try {
                saveObject(userProfiles, "userMetaWordProfiles");
                System.out.println("UserProfile mentese sikeres!");
            } catch (IOException e) {
                System.out.println("UserProfile mentese sikertelen:");
                e.printStackTrace();
            }
        }
    }

    public TObjectDoubleHashMap<String> createUserMetaWordProfile(int uIdx) {

        TCharIntHashMap numOfKeyValuesByKey = new TCharIntHashMap(keys.length);
        for(String key : keys)
            numOfKeyValuesByKey.put(key.charAt(0),1);

        int numItems = 0;
        TObjectDoubleHashMap<String> wordCounts = new TObjectDoubleHashMap<>(100);
        TObjectDoubleHashMap<String> wordItemAppearences = new TObjectDoubleHashMap<>(100);

        for (Database.Event e : db.userEvents(uIdx, null)) {
            numItems++;
            HashSet<String> appearedWordsInItem = new HashSet<>(100);
            for (String key : keys) {
                String keyAll = dbExt.getItemKeyValue(e.iIdx, key);
                String[] values = keyAll.split(KEY_VALUE_LIST_SEPARATOR); // /f és / jel menten felbontva
                char keyType = key.charAt(0);
                for (String val : values) {
                    if (val.equals(""))
                        continue;
                    numOfKeyValuesByKey.put(keyType, numOfKeyValuesByKey.get(keyType) + 1);
                    String mWrd = keyType + ":" + val.toLowerCase();
                    appearedWordsInItem.add(mWrd);
                    wordCounts.put(mWrd, wordCounts.get(mWrd) + 1);
                }
            }

            for(String word : appearedWordsInItem) {
                wordItemAppearences.put(word, wordItemAppearences.get(word) + 1);
            }
        }


        //TF számítása

        TObjectDoubleIterator<String> iterator = wordCounts.iterator();
        while (iterator.hasNext()) {
            iterator.advance();
            double val = iterator.value();
            String word = iterator.key();
            char c = word.charAt(0);
            int keyValueNum = numOfKeyValuesByKey.get(c);
            wordCounts.put(word,val / keyValueNum);
        }

        //IDF Számítása

        iterator = wordItemAppearences.iterator();
        while (iterator.hasNext()) {
            iterator.advance();
            double val = iterator.value();
            String word = iterator.key();
            char c = word.charAt(0);
            wordItemAppearences.put(word, Math.log((double) numItems / val));
        }

        // TFIDF = TF * IDF


        TObjectDoubleHashMap<String> newWeights = new TObjectDoubleHashMap(wordCounts.size()); //uj sulyok

        iterator = wordCounts.iterator();
        while (iterator.hasNext()) {
            iterator.advance();
            double val = iterator.value();
            String word = iterator.key();
            newWeights.put(word, val * wordItemAppearences.get(word));
        }

        return newWeights;
    }

    public void printSuppsToFile(String type) {
        PrintWriter out = null;
        if (type.equals("metaWordSuppsByEvents")) {
            try {
                System.out.println("Printing support of metawords...");
                out = new PrintWriter(metaWordSuppsByEventFileName);
                TObjectIntIterator<String> iterator = metaWordSuppsByEvents.iterator();
                while (iterator.hasNext()) {
                    iterator.advance();
                    out.println(iterator.key() + ":\t\t\t\t" + iterator.value());
                }
                System.out.println("Printing metaword supps is successful!");
            } catch (Exception e) {
                System.out.println("Printing metaword supps is unsuccessful:");
                System.err.println(e.getMessage());
            } finally {
                out.close();
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
                System.out.println("Printing metaword supps is successful!");
            } catch (Exception e) {
                System.out.println("Printing metaword supps is unsuccessful:");
                System.err.println(e.getMessage());
            } finally {
                out.close();
            }
        } else if (type.equals("metaWordIndices")) {
            try {
                System.out.println("Printing indices of metawords...");
                out = new PrintWriter(metaWordIndicesFileName);
                TObjectIntIterator<String> iterator = metaWords.iterator();
                while (iterator.hasNext()) {
                    iterator.advance();
                    out.println(iterator.key() + ":\t\t\t\t" + iterator.value());
                }
                System.out.println("Printing metaword indices is successful!");
            } catch (Exception e) {
                System.out.println("Printing metaword indices is unsuccessful:");
                System.err.println(e.getMessage());
            } finally {
                out.close();
            }
        }
    }

    @Override
    public double predict(int uIdx, int iIdx, long time) {

        double prediction = 0.0;

        TObjectDoubleHashMap<String> profWeights = userProfiles.get(uIdx);

        for(String key : keys){
            String keyAll = dbExt.getItemKeyValue(iIdx, key);
            String[] values = keyAll.split(KEY_VALUE_LIST_SEPARATOR); // /f és / jel menten felbontva
            for (String val : values) {
                if (val.equals(""))
                    continue;
                char c = key.charAt(0);
                String mWrd = c + ":" + val.toLowerCase();
                if(profWeights.containsKey(mWrd)){
                    prediction = prediction + profWeights.get(mWrd);
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
