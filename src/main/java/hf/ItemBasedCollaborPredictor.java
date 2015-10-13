package hf;


import gnu.trove.iterator.TIntIntIterator;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.iterator.TObjectDoubleIterator;
import gnu.trove.iterator.TObjectIntIterator;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.map.hash.TObjectDoubleHashMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import gnu.trove.set.hash.TIntHashSet;
import onlab.core.Database;
import onlab.core.ExtendedDatabase;
import onlab.core.evaluation.Evaluation;
import onlab.core.predictor.Predictor;
import onlab.core.util.Util;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;

public class ItemBasedCollaborPredictor extends Predictor {

    public static final String META_KEYS = "metaKeys";
    private static final String KEYVALUE_WEIGHT_SEP = ":";
    private static final String itemToItemSimilaritiesFileName = "itemToItemSimilarities";
    private static final String itemToItemSuppsFileName = "itemToItemSupps";
    private static final String itemSuppsFileName = "itemSupps";
    private static final String itemsSeenByUsersFileName = "itemsSeenByUsers";
    private static final String itemToItemSetsFileName = "itemToItemSets";

    //region Other member variables

    private static double LAMBDA = 0.0;

    /**
     * A leghasonlobb N elem
     */
    private static final int topN = 10;

    public ExtendedDatabase dbExt = null;

    /**
     * Minden felhasznalora, hogy mely filmeket latta
     */
    private TIntObjectHashMap<TIntHashSet> itemsSeenByUsers;

    /**
     * Mely itemet melyik itemmel neztek egyutt
     */
    private TIntObjectHashMap<TIntHashSet> itemToItemSets;

    /**
     * Mely itemet mely itemmel hanyszor vettek egyutt
     */
    private TObjectIntHashMap<Pair> itemToItemSupps;

    /**
     * Egymashoz 0-nal nagyobb hasonlosaggal rendelkezo filmparok
     */
    private TObjectDoubleHashMap<Pair> itemToItemSimilarities;

    /**
     * supp(item) minden Item-re
     */
    private TIntIntHashMap itemSupps;

    /**
     * Predikcios folyamat nyomonkovetesere
     */
    private int testPercent;

    //endregion


    @Override
    protected void initParameters() {
        par.put(META_KEYS, "");
    }

    @Override
    public void train(Database db, Evaluation eval) {
        System.out.println("Started training...");
        this.db = db;

        dbExt = (ExtendedDatabase) db;

        testPercent = 1;

        String[] kv = par.get(META_KEYS).split(KEYVALUE_WEIGHT_SEP);
        LAMBDA = Double.parseDouble(kv[1]);


        try {
            if (!isFileExists("itemToItemSimilaritiesTXT")) {
                if (!isFileExists("itemToItemSuppsTXT") || !isFileExists("itemSuppsTXT") || !isFileExists("itemsSeenByUsersBIN") || !isFileExists("itemToItemSetsBIN")) {
                    countItemSupps(true, true, true, true);
                } else {
                    loadItemInfoFromFile("itemSuppsTXT");
                    loadItemInfoFromFile("itemToItemSuppsTXT");
                    loadItemInfoFromFile("itemsSeenByUsersBIN");
                }
                computeItemSims(true);
            } else {
                loadItemInfoFromFile("itemsSeenByUsersBIN");
                loadItemInfoFromFile("itemToItemSimilaritiesTXT");
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }

//        listTopNItemToItem(55);
//        listTopNItemToItem(200);
//        listTopNItemToItem(357);
//        listTopNItemToItem(450);
//        listTopNItemToItem(500);
//        listTopNItemToItem(660);
    }

    /**
     * Eventek alapjan itemSupport, �s item-to-item support szamitas
     */
    public void countItemSupps(boolean printItemsByUsersToFile, boolean printSuppsToFile, boolean printPairSuppsToFile, boolean printItemToItemSets ) {
        System.out.println("Item pair support szamitasa...");
        int nEvents = 0;
        int percent = 1;
        int size = db.numEvents();
        int percent1 = size / 100;
        itemsSeenByUsers = new TIntObjectHashMap(db.numUsers());
        itemToItemSets = new TIntObjectHashMap<TIntHashSet>();
        itemToItemSupps = new TObjectIntHashMap<Pair>();
        itemSupps = new TIntIntHashMap();
        for (Database.Event e : db.events(null)) {
            nEvents++;
            int iIdx = e.iIdx;
            int uIdx = e.uIdx;

            itemSupps.put(iIdx, itemSupps.get(iIdx) + 1);

            TIntHashSet allItemsOfActualUser;
            if (!itemsSeenByUsers.containsKey(uIdx)) {
                allItemsOfActualUser = new TIntHashSet();
                itemsSeenByUsers.put(uIdx, allItemsOfActualUser);
            } else {
                allItemsOfActualUser = itemsSeenByUsers.get(uIdx);
            }

            boolean isUserSetContainsItem = allItemsOfActualUser.contains(iIdx);
            // ami benne volt:
            TIntIterator iterator = allItemsOfActualUser.iterator();
            while (iterator.hasNext()) {
                int item = iterator.next();
                if (!isUserSetContainsItem) {
                    linkItems(item, iIdx);
                }
                incrementPairSupp(item, iIdx);
            }

            if (!isUserSetContainsItem) {
                allItemsOfActualUser.add(iIdx);
            }

            if (nEvents == percent1) {
                System.out.print(percent + "% ");
                percent++;
                nEvents = 0;
            }
        }
        if (printItemsByUsersToFile) {
            printSuppsToFile("itemsSeenByUsersBIN");
        }
        if (printSuppsToFile) {
            printSuppsToFile("itemSuppsBIN");
            printSuppsToFile("itemSuppsTXT");
        }
        if (printPairSuppsToFile) {
            printSuppsToFile("itemToItemSuppsBIN");
            printSuppsToFile("itemToItemSuppsTXT");
        }
        if (printItemToItemSets) {
            printSuppsToFile("itemToItemSetsBIN");
        }
    }

    private void incrementPairSupp(int item1, int item2){
        if (item1 == item2) return;
        Pair pair = new Pair(item1, item2);
        itemToItemSupps.put(pair, itemToItemSupps.get(pair) + 1);
    }

    private void linkItems(int item1, int item2) {
        if (item1 == item2) return;
        getLinkedItems(item1).add(item2);
        getLinkedItems(item2).add(item1);
        incrementPairSupp(item1, item2);
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

    public void listTopNItemToItem(int item){
        if (itemToItemSets == null) {
            try {
                loadItemInfoFromFile("itemToItemSetsBIN");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        //TODO Hogy fordulhat elo Nincs ilyen item hibauzi?
        TIntHashSet itemSet =  itemToItemSets.get(item);
        if(itemSet == null){
            System.out.println("Nincs ilyen item!");
            return;
        }

        ArrayList<ObjectSim> list = new ArrayList<ObjectSim>();
        TIntIterator iterator = itemSet.iterator();
        while(iterator.hasNext()){
            int _item = iterator.next();
            double similarity = itemToItemSimilarities.get(new Pair(item, _item));
            list.add(new ObjectSim(_item, similarity));
        }
        Collections.sort(list);

        int numOfSims = list.size();
        int i = topN > numOfSims ? numOfSims : topN;
        int j = 0;
        System.out.println(dbExt.getItem(item).name + " leghasonlobb " + i + " tipusfuggetlen szava:");
        while (i-- > 0) {
            ObjectSim o = list.get(j);
            System.out.println(j + ".:\t" + dbExt.getItem(o.idx).name + "\t" + o.sim);
            j++;
        }
    }

    public void printSuppsToFile(String type) {
        PrintWriter out = null;
        if (type.equals("itemSuppsTXT")) {
            try {
                System.out.println("Printing support of items...");
                out = new PrintWriter(itemSuppsFileName + ".txt");
                TIntIntIterator iterator = itemSupps.iterator();
                while (iterator.hasNext()) {
                    iterator.advance();
                    out.println(iterator.key() + "\t" + iterator.value());
                }
                System.out.println("Printing supps is successful!");
            } catch (Exception e) {
                System.err.println(e.getMessage());
            } finally {
                if (out != null) {
                    out.close();
                }
            }
        } else if (type.equals("itemSuppsBIN")) {
            try {
                System.out.println("Printing support of items to BIN...");
                saveObject(itemSupps, itemSuppsFileName);
                System.out.println("Printing supps is successful!");
            } catch (IOException e) {
                System.out.println("Printing supps is unsuccessful!");
                e.printStackTrace();
            }
        } else if (type.equals("itemToItemSuppsTXT")) {
            try {
                System.out.println("Printing support of itemToItemSupps into file " + itemToItemSuppsFileName);
                out = new PrintWriter(itemToItemSuppsFileName + ".txt");
                TObjectIntIterator<Pair> iterator = itemToItemSupps.iterator();
                while (iterator.hasNext()) {
                    iterator.advance();
                    Pair p = iterator.key();
                    out.println(p.iIdx1 + "\t" + p.iIdx2 + "\t" + iterator.value());
                }
                System.out.println("Printing itempairsupps is successful!");
            } catch (Exception e) {
                System.err.println(e.getMessage());
            } finally {
                if (out != null) {
                    out.close();
                }
            }
        } else if (type.equals("itemsSeenByUsersBIN")) {
            try {
                System.out.println("Printing items seen by users into file " + itemsSeenByUsersFileName);
                saveObject(itemsSeenByUsers, itemsSeenByUsersFileName);
                System.out.println("itemsSeenByUsers mentese sikeres!");
            } catch (IOException e) {
                System.out.println("itemsSeenByUsers mentese sikertelen:");
                e.printStackTrace();
            }
        } else if (type.equals("itemToItemSimilaritiesTXT")) {
            try {
                System.out.println("Printing itemToItemSimilarities into TXT " + itemToItemSimilaritiesFileName + ".txt");
                out = new PrintWriter(itemToItemSimilaritiesFileName + ".txt");
                TObjectDoubleIterator<Pair> iterator = itemToItemSimilarities.iterator();
                while (iterator.hasNext()) {
                    iterator.advance();
                    Pair p = iterator.key();
                    out.println(p.iIdx1 + "\t" + p.iIdx2 + "\t" + iterator.value());
                }
                System.out.println("Printing itemToItemSimilarities is successful!");
            } catch (Exception e) {
                System.out.println("Printing itemToItemSimilarities is unsuccessful:");
                System.err.println(e.getMessage());
            } finally {
                if (out != null) {
                    out.close();
                }
            }
        } else if (type.equals("itemToItemSimilaritiesBIN")) {
            System.out.println("itemToItemSimilarities mentese BINbe");
            try {
                saveObject(itemToItemSimilarities, itemToItemSimilaritiesFileName);
                System.out.println("itemToItemSimilarities mentese sikeres!");
            } catch (IOException e) {
                System.out.println("itemToItemSimilarities mentese sikertelen:");
                e.printStackTrace();
            }
        } else if (type.equals("itemToItemSetsBIN")) {
            System.out.println("itemToItemSets mentese BINbe");
            try {
                saveObject(itemToItemSets, itemToItemSetsFileName);
                System.out.println("itemToItemSets mentese sikeres!");
            } catch (IOException e) {
                System.out.println("itemToItemSets mentese sikertelen:");
                e.printStackTrace();
            }
        } else try {
            throw new Exception("Nincs ilyen betoltesi parancs: " + type);
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

    public void computeItemSims(boolean printSimsToFile) {
        System.out.println("itemToItemSimilarity szamitas!");
        int nPairs = 0;
        int percent = 1;
        int size = itemToItemSupps.size();
        int percent1 = size / 100;
        itemToItemSimilarities = new TObjectDoubleHashMap<Pair>(size);
        TObjectIntIterator<Pair> iterator = itemToItemSupps.iterator();
        while (iterator.hasNext()) {
            nPairs++;
            iterator.advance();
            Pair p = iterator.key();
            int suppAB = iterator.value();
            int suppA = itemSupps.get(p.iIdx1);
            int suppB = itemSupps.get(p.iIdx2);

            double sim;


//            sim = (double) suppAB
//                    / (Math.sqrt(suppA + LAMBDA) * Math
//                    .sqrt(suppB + LAMBDA));

            //Jaccard
            sim = (double) suppAB
                    / (suppA + suppB - suppAB);
            itemToItemSimilarities.put(p, sim);



            if (nPairs == percent1) {
                System.out.print(percent + "% ");
                percent++;
                nPairs = 0;
            }
        }

        if (printSimsToFile) {
            printSuppsToFile("itemToItemSimilaritiesTXT");
            printSuppsToFile("itemToItemSimilaritiesBIN");
        }
        System.out.println("itemToItemSimilarity szamitas KESZ");
    }

    public boolean isFileExists(String filetype) throws FileNotFoundException {
        if (filetype.equals("itemToItemSimilaritiesTXT")) {
            File f = new File(itemToItemSimilaritiesFileName + ".txt");
            return f.exists() && f.isFile();
        } else if (filetype.equals("itemToItemSimilaritiesBIN")) {
            File f = new File(itemToItemSimilaritiesFileName);
            return f.exists() && f.isFile();
        } else if (filetype.equals("itemToItemSuppsTXT")) {
            File f = new File(itemToItemSuppsFileName + ".txt");
            return f.exists() && f.isFile();
        } else if (filetype.equals("itemToItemSuppsBIN")) {
            File f = new File(itemToItemSuppsFileName);
            return f.exists() && f.isFile();
        } else if (filetype.equals("itemSuppsBIN")) {
            File f = new File(itemSuppsFileName);
            return f.exists() && f.isFile();
        } else if (filetype.equals("itemSuppsTXT")) {
            File f = new File(itemSuppsFileName + ".txt");
            return f.exists() && f.isFile();
        } else if (filetype.equals("itemsSeenByUsersBIN")) {
            File f = new File(itemsSeenByUsersFileName);
            return f.exists() && f.isFile();
        } else if (filetype.equals("itemToItemSetsBIN")) {
            File f = new File(itemToItemSetsFileName);
            return f.exists() && f.isFile();
        } else throw new FileNotFoundException("Hibas fajltipus!");
    }

    public void loadItemInfoFromFile(String itemInfoType) throws Exception {
        BufferedReader reader = null;
        if (itemInfoType.equals("itemSuppsTXT")) {
            System.out.println("Loading item Supports from TXT...");
            itemSupps = new TIntIntHashMap(14000);
            try {
                reader = new BufferedReader(new FileReader(itemSuppsFileName + ".txt"));
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] splittedLine = line.split("\t");
                    itemSupps.put(Integer.parseInt(splittedLine[0]), Integer.parseInt(splittedLine[1]));
                }
                System.out.println("Loading item Supports done!");
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("Loading item Supports failed!");
            } finally {
                if (reader != null) {
                    reader.close();
                }
            }
        } else if (itemInfoType.equals("itemSuppsBIN")) {
            System.out.println("itemSuppsBIN betoltese");
            try {
                itemSupps = (TIntIntHashMap) loadObject(itemSuppsFileName);
                System.out.println("itemSuppsBIN betoltese sikeres!");
            } catch (IOException e) {
                System.out.println("itemSuppsBIN betoltese sikertelen:");
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                System.out.println("itemSuppsBIN betoltese sikertelen:");
                e.printStackTrace();
            }
        } else if (itemInfoType.equals("itemToItemSetsBIN")) {
            System.out.println("itemToItemSetsBIN betoltese");
            try {
                itemToItemSets = (TIntObjectHashMap<TIntHashSet>) loadObject(itemToItemSetsFileName);
                System.out.println("itemToItemSetsBIN betoltese sikeres!");
            } catch (IOException e) {
                System.out.println("itemToItemSetsBIN betoltese sikertelen:");
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                System.out.println("itemToItemSetsBIN betoltese sikertelen:");
                e.printStackTrace();
            }
        } else if (itemInfoType.equals("itemToItemSuppsTXT")) {

            System.out.println("Loading itemToItemSupps from TXT...");
            int importedRows = 0;
            int percent = 1;
            itemToItemSupps = new TObjectIntHashMap(16500000);
            try {
                reader = new BufferedReader(new FileReader(itemToItemSuppsFileName + ".txt"));
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] splittedLine = line.split("\t");
                    itemToItemSupps.put(new Pair(Integer.parseInt(splittedLine[0]), Integer.parseInt(splittedLine[1])), Integer.parseInt(splittedLine[2]));
                    importedRows++;
                    if (importedRows == 163070) {
                        System.out.print(percent + "%  ");
                        importedRows = 0;
                        percent++;
                    }
                }
                System.out.println("Loading itemToItemSupps done!");
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("Loading itemToItemSupps failed!");
            } finally {
                if (reader != null) {
                    reader.close();
                }
            }
        } else if (itemInfoType.equals("itemToItemSuppsBIN")) {
            System.out.println("itemToItemSuppsBIN betoltese");
            try {
                itemSupps = (TIntIntHashMap) loadObject(itemToItemSuppsFileName);
                System.out.println("itemSuppsBIN betoltese sikeres!");
            } catch (IOException e) {
                System.out.println("itemSuppsBIN betoltese sikertelen:");
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                System.out.println("itemSuppsBIN betoltese sikertelen:");
                e.printStackTrace();
            }
        } else if (itemInfoType.equals("itemToItemSimilaritiesTXT")) {
            System.out.println("Loading itemToItemSimilarities from TXT...");
            int importedRows = 0;
            int percent = 1;
            itemToItemSimilarities = new TObjectDoubleHashMap(20500000);
            try {
                reader = new BufferedReader(new FileReader(itemToItemSimilaritiesFileName + ".txt"));
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] splittedLine = line.split("\t");
                    itemToItemSimilarities.put(new Pair(Integer.parseInt(splittedLine[0]), Integer.parseInt(splittedLine[1])), Double.parseDouble(splittedLine[2]));
                    importedRows++;
                    if (importedRows == 163070) {
                        System.out.print(percent + "%  ");
                        importedRows = 0;
                        percent++;
                    }
                }
                System.out.println("Loading itemToItemSimilarities done!");
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("Loading itemToItemSimilarities failed!");
            } finally {
                if (reader != null) {
                    reader.close();
                }
            }
        } else if (itemInfoType.equals("itemToItemSimilaritiesBIN")) {
            System.out.println("itemToItemSimilarity betoltese");
            try {
                itemToItemSimilarities = (TObjectDoubleHashMap<Pair>) loadObject(itemToItemSimilaritiesFileName);
                System.out.println("itemToItemSimilarity betoltese sikeres!");
            } catch (IOException e) {
                System.out.println("itemToItemSimilarity betoltese sikertelen:");
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                System.out.println("itemToItemSimilarity betoltese sikertelen:");
                e.printStackTrace();
            }
        } else if (itemInfoType.equals("itemsSeenByUsersBIN")) {
            System.out.println("itemsSeenByUsers betoltese");
            try {
                itemsSeenByUsers = (TIntObjectHashMap<TIntHashSet>) loadObject(itemsSeenByUsersFileName);
                System.out.println("itemsSeenByUsers betoltese sikeres!");
            } catch (IOException e) {
                System.out.println("itemsSeenByUsers betoltese sikertelen:");
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                System.out.println("itemsSeenByUsers betoltese sikertelen:");
                e.printStackTrace();
            }
        } else throw new Exception("Nincs ilyen betoltesi parancs!");
    }

    TIntHashSet itemsSeenByLastUser;
    int lastUser = -1;
    int numOfUsers = 0;

    @Override
    public double predict(int uIdx, int iIdx, long time) {

        double prediction = 0.0;

        if (lastUser != uIdx) {
            TIntHashSet tIntHashSet = itemsSeenByUsers.get(uIdx);
            if (tIntHashSet != null) {
                itemsSeenByLastUser = tIntHashSet;
            } else {
                itemsSeenByLastUser = null;
            }
            lastUser = uIdx;
            numOfUsers++;
        }

        if (itemsSeenByLastUser != null) {
            TIntIterator iterator = itemsSeenByLastUser.iterator();
            while (iterator.hasNext()) {
                int itemID = iterator.next();
                //TODO Felülvizsgálni ezt a képletet
                double similarity = itemID == iIdx ? 1.0 : itemToItemSimilarities.get(new Pair(itemID, iIdx));
                prediction += similarity;
            }
        }

        if (uIdx / 10000 > testPercent) {
            System.out.print(testPercent + " ");
            testPercent++;
        }
        return prediction;
    }
}
