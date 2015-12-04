package hf.GraphUtils;

import onlab.core.Database;
import onlab.core.ExtendedDatabase;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.util.*;


public class GraphDBBuilder {

    private static HashMap<Labels,HashMap<Object, Long>> IDToIDs;
    private static ExtendedDatabase dbExt;
    private static BatchInserter batchInserter;
    public static final String actor = "Actor";
    public static final String director = "Director";
    public static final String VOD = "VodMenuDirect";
    private static final String act_dir_separator = "\f";
    private static final String vod_separator = "[\f/]";
    private static final String stopWordsFileName = "stopwords.txt";
    private static HashSet<String> stopWords;

    public static void buildGraphDBFromImpressDB(GraphDB graphDB, boolean deleteIfExists) throws IOException {

        if(graphDB.isInited())
            graphDB.shutDownDB();

        LogHelper.INSTANCE.logToFileT("GraphDB építés kezdése: ");

        dbExt = (ExtendedDatabase) graphDB.db;
        IDToIDs = new HashMap<>();

        //Delete if exists
        if(graphDB.getDbFolder().exists() && deleteIfExists)
            FileUtils.deleteRecursively(graphDB.getDbFolder());

        //MetaWord Stoplist
        stopWords = (graphDB.filterStopWords) ? loadStopWordsFromFile() : null;

        try {
            batchInserter = BatchInserters.inserter(graphDB.getDbFolder().getAbsoluteFile());

            //Creating indexes
            for(Labels l : Labels.values()) {
                if (!l.getIDName().equals(""))
                    batchInserter.createDeferredSchemaIndex(l).on(l.getIDName()).create();
                if (!l.getPropertyName().equals(""))
                    batchInserter.createDeferredSchemaIndex(l).on(l.getPropertyName()).create();
            }

            //Nodes:

            //Items
            insertNodesAndSaveGraphIDs(Labels.Item,getItemsFromDB());
            //Users
            insertNodesAndSaveGraphIDs(Labels.User,getUsersFromDB());
            //Actors
            insertNodesAndSaveGraphIDs(Labels.Actor,getKeyValuesFromDB(actor,act_dir_separator,Labels.Actor));
            //Directors
            insertNodesAndSaveGraphIDs(Labels.Director,getKeyValuesFromDB(director,act_dir_separator,Labels.Director));
            //VOD
            insertNodesAndSaveGraphIDs(Labels.VOD,getKeyValuesFromDB(VOD,vod_separator,Labels.VOD));

            //Relationships:

            //SEEN
            if(graphDB.uniqueEvents)
                insertRelationshipsWithoutProperties(Relationships.SEEN, getUniqueSEENRelationships());
            else
                insertRelationshipsWithoutProperties(Relationships.SEEN, getSEENRelationships());
            //ACTS_IN
            insertRelationshipsWithoutProperties(Relationships.ACTS_IN,
                    getMetaRelationships(actor,act_dir_separator,Labels.Actor));
            //DIR_BY
            insertRelationshipsWithoutProperties(Relationships.DIR_BY,
                    getMetaRelationships(director,act_dir_separator,Labels.Director));
            //HAS_META
            insertRelationshipsWithoutProperties(Relationships.HAS_META,
                    getMetaRelationships(VOD,vod_separator,Labels.VOD));
            IDToIDs.remove(Labels.Item);

            //IDF values:
            graphDB.initDB();
            graphDB.computeAndUploadIDFValues(Labels.Actor,Relationships.ACTS_IN);
            graphDB.computeAndUploadIDFValues(Labels.Director,Relationships.DIR_BY);
            graphDB.computeAndUploadIDFValues(Labels.VOD,Relationships.HAS_META);
            graphDB.shutDownDB();
        }
        finally {
            if ( batchInserter != null )
            {
                batchInserter.shutdown();
                LogHelper.INSTANCE.logToFileT("GraphDB felépítve!");
            }
        }
    }

    private static void insertNodesAndSaveGraphIDs(Labels l, ArrayList<Map<String,Object>> nodes){
        LogHelper.INSTANCE.logToFileT(l.name() + " list keszitese: ");
        HashMap<Object, Long> actualMap = new HashMap<>();
        int num = 0;
        for(; num < nodes.size(); num++){
            Map<String,Object> nodeProps = nodes.get(num);
            long graphDBID = batchInserter.createNode(nodeProps,l);
            actualMap.put(nodeProps.get(l.getUniqueName()),graphDBID);
        }
        IDToIDs.put(l,actualMap);
        LogHelper.INSTANCE.logToFileT(l.name() +" list keszitese KESZ: " + num);
    }

    private static ArrayList<Map<String, Object>> getItemsFromDB() {
        ArrayList<Map<String, Object>> items = new ArrayList<>(dbExt.numItems());
        for (Database.Item i : dbExt.items(null)) {
            Map<String, Object> nodeProps = new HashMap<>();
            int itemID = dbExt.getItemId(i.idx);
            nodeProps.put(Labels.Item.getIDName(),itemID);
            nodeProps.put(Labels.Item.getPropertyName(),i.name);
            items.add(nodeProps);
        }
        return items;
    }

    private static ArrayList<Map<String, Object>> getUsersFromDB() {
        ArrayList<Map<String, Object>> users = new ArrayList<>(dbExt.numUsers());
        for (Database.User u : dbExt.users(null)) {
            Map<String, Object> nodeProps = new HashMap<>();
            int userID = dbExt.getUserId(u.idx);
            nodeProps.put(Labels.User.getIDName(),userID);
            users.add(nodeProps);
        }
        return users;
    }

    private static ArrayList<Map<String, Object>> getKeyValuesFromDB(String keyValueKEY, String splitPattern, Labels l) {
        HashSet<String> keyValues = new HashSet<>();
        for (Database.Item i : dbExt.items(null)) {
            keyValues.addAll(GraphDBBuilder.getUniqueItemMetaWordsByKey(dbExt, dbExt.getItemId(i.idx), keyValueKEY, stopWords));
        }
        return createMapListFromSet(l,keyValues);
    }

    private static ArrayList<Map<String, Object>> createMapListFromSet(Labels l, HashSet<String> hs){
        ArrayList<Map<String, Object>> info = new ArrayList<>(hs.size());
        for (String s : hs) {
            Map<String, Object> nodeProps = new HashMap<>();
            nodeProps.put(l.getPropertyName(),s);
            info.add(nodeProps);
        }
        return info;
    }

    private static void insertRelationshipsWithoutProperties(Relationships rel, ArrayList<DirectedLink<Long>> links){
        LogHelper.INSTANCE.logToFileT(rel.name() + " list keszitese: ");
        int num = 0;
        for(DirectedLink<Long> l : links){
            batchInserter.createRelationship(l.startNode,l.endNode,rel,null);
            num++;
        }
        LogHelper.INSTANCE.logToFileT(rel.name() +" list keszitese KESZ: " + num);
    }

    private static ArrayList<DirectedLink<Long>> getUniqueSEENRelationships() {
        HashSet<DirectedLink<Long>> uniqueEvents = new HashSet<>(dbExt.numEvents());
        HashMap<Object,Long> userIDToIDPairs = IDToIDs.get(Labels.User);
        HashMap<Object,Long> itemIDToIDPairs = IDToIDs.get(Labels.Item);
        for (Database.Event e : dbExt.events(null)) {
            long userGraphDBID = userIDToIDPairs.get(dbExt.getUserId(e.uIdx));
            long itemGraphDBID = itemIDToIDPairs.get(dbExt.getItemId(e.iIdx));
            uniqueEvents.add(new DirectedLink<>(userGraphDBID,itemGraphDBID));
        }
        IDToIDs.remove(Labels.User);    //betöltve minden, ami hozzájuk kapcsolódik, ezért törlöm

        ArrayList<DirectedLink<Long>> events = new ArrayList<>(uniqueEvents.size());
        for(DirectedLink l : uniqueEvents){
            events.add(l);
        }
        return events;
    }

    private static ArrayList<DirectedLink<Long>> getSEENRelationships() {
        ArrayList<DirectedLink<Long>> events = new ArrayList<>(dbExt.numEvents());
        HashMap<Object,Long> userIDToIDPairs = IDToIDs.get(Labels.User);
        HashMap<Object,Long> itemIDToIDPairs = IDToIDs.get(Labels.Item);
        for (Database.Event e : dbExt.events(null)) {
            long userGraphDBID = userIDToIDPairs.get(dbExt.getUserId(e.uIdx));
            long itemGraphDBID = itemIDToIDPairs.get(dbExt.getItemId(e.iIdx));
            events.add(new DirectedLink<>(userGraphDBID,itemGraphDBID));
        }
        IDToIDs.remove(Labels.User);    //betöltve minden, ami hozzájuk kapcsolódik, ezért törlöm
        return events;
    }

    private static ArrayList<DirectedLink<Long>> getMetaRelationships(String keyValueKEY, String splitPattern, Labels label) {
        ArrayList<DirectedLink<Long>> metaRel = new ArrayList<>(dbExt.numItems());
        HashMap<Object,Long> metaIDToIDPairs = IDToIDs.get(label);
        HashMap<Object,Long> itemIDToIDPairs = IDToIDs.get(Labels.Item);
        for (Database.Item i : dbExt.items(null)) {
            int iID = dbExt.getItemId(i.idx);
            HashSet<String> metas = GraphDBBuilder.getUniqueItemMetaWordsByKey(dbExt, iID, keyValueKEY, stopWords);
            long itemGraphDBID = itemIDToIDPairs.get(iID);
            for(String s : metas){
                long metaGraphDBID = metaIDToIDPairs.get(s);
                metaRel.add(new DirectedLink<>(itemGraphDBID,metaGraphDBID));
            }
        }
        IDToIDs.remove(label);
        return metaRel;
    }

    public static HashSet<String> getUniqueItemMetaWordsByKey(Database db, int iID, String key, HashSet<String> stopWords) {
        ExtendedDatabase dbExt = (ExtendedDatabase) db;
        HashSet<String> itemMetaWords = new HashSet<>();
        String keyAll =  dbExt.getItemKeyValue(dbExt.getItemIndex(iID) , key);
        String key_value_separator = (key.equals(GraphDBBuilder.VOD)) ? vod_separator : act_dir_separator;
        String[] values = keyAll.split(key_value_separator);
        for (String val : values) {
            if (checkCondition(val, key, stopWords))
                itemMetaWords.add(val);
        }
        return itemMetaWords;
    }

    /**
     * Grafba toltes elott metaword ellenorzese
     * @param word Ellenorizendo metaword
     */
    public static boolean checkCondition(String word, String metaType, HashSet<String> stopWords) {
        if(stopWords != null) {
            if (!word.equals("") && word.length() > 2 && !stopWords.contains(word)) {
                return true;
            }
        }
        else {
            switch (metaType) {
                case actor:
                    if (!word.equals("") && !word.equals("NA") && !word.equals("na") && !word.equals("n/a") && !word.equals("N\\A") && !word.equals("N/A"))
                        return true;
                    break;
                case director:
                    if (!word.equals("") && !word.equals("NA") && !word.equals("na") && !word.equals("n/a") && !word.equals("N\\A") && !word.equals("N/A") && !word.equals("n/d"))
                        return true;
                    break;
                case VOD:
                    if (!word.equals("") && word.length() > 2)
                        return true;
                    break;
            }
        }
        return false;
    }

    public static HashSet<String> loadStopWordsFromFile() {
        HashSet<String> stopWords = new HashSet<>();
        try {
            Scanner stopFile = new Scanner(new FileReader(stopWordsFileName));
            while(stopFile.hasNext()){
                stopWords.add(stopFile.nextLine());
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        return stopWords;
    }
}
