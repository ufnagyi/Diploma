package hf;

import onlab.core.Database;
import onlab.core.ExtendedDatabase;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;


public class GraphDBBuilder {

    private static HashMap<Labels,HashMap<Object, Long>> IDToIDs;
    private static ExtendedDatabase dbExt;
    private static BatchInserter batchInserter;

    public static void buildGraphDBFromImpressDB(GraphDB graphDB, Database db, boolean deleteIfExists) throws IOException {
        LogHelper.INSTANCE.log("GraphDB építés kezdése: ");

        dbExt = (ExtendedDatabase) db;
        IDToIDs = new HashMap<>();

        //Delete if exists
        if(graphDB.getDbFolder().exists() && deleteIfExists)
            FileUtils.deleteRecursively(graphDB.getDbFolder());

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
            insertNodesAndSaveGraphIDs(Labels.Actor,getActorsFromDB());
            //Directors
            insertNodesAndSaveGraphIDs(Labels.Director,getDirectorsFromDB());
            //VOD
            insertNodesAndSaveGraphIDs(Labels.VOD,getVODsFromDB());

            //Relationships:

            //SEEN
            insertRelationshipsWithoutProperties(Relationships.SEEN,getSEENRelationships());
            //ACTS_IN
            insertRelationshipsWithoutProperties(Relationships.ACTS_IN,getACTSINRelationships());
            //DIR_BY
            insertRelationshipsWithoutProperties(Relationships.DIR_BY,getDIRBYRelationships());
            //HAS_META
            insertRelationshipsWithoutProperties(Relationships.HAS_META,getHASMETARelationships());
        }
        finally {
            if ( batchInserter != null )
            {
                batchInserter.shutdown();
                LogHelper.INSTANCE.log("GraphDB felépítve!");
            }
        }
    }

    private static void insertNodesAndSaveGraphIDs(Labels l, ArrayList<Map<String,Object>> nodes){
        LogHelper.INSTANCE.log(l.name() + " list keszitese: ");
        HashMap<Object, Long> actualMap = new HashMap<>();
        int num = 0;
        for(; num < nodes.size(); num++){
            Map<String,Object> nodeProps = nodes.get(num);
            long graphDBID = batchInserter.createNode(nodeProps,l);
            actualMap.put(nodeProps.get(l.getUniqueName()),graphDBID);
        }
        IDToIDs.put(l,actualMap);
        LogHelper.INSTANCE.log(l.name() +" list keszitese KESZ: " + num);
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

    private static ArrayList<Map<String, Object>> getActorsFromDB() {
        HashSet<String> actors = new HashSet<>();
        for (Database.Item i : dbExt.items(null)) {
            for (String actor : dbExt.getItemKeyValue(i.idx, "Actor").split("\f")) {
                if (!actor.equals("") && !actor.equals("NA") && !actor.equals("na") && !actor.equals("n/a") && !actor.equals("N\\A") && !actor.equals("N/A"))
                    actors.add(actor);
            }
        }
        return createMapListFromSet(Labels.Actor,actors);
    }

    private static ArrayList<Map<String, Object>> getDirectorsFromDB() {
        HashSet<String> directors = new HashSet<>();
        for (Database.Item i : dbExt.items(null)) {
            for (String director : dbExt.getItemKeyValue(i.idx, "Director").split("\f")) {
                if (!director.equals("") && !director.equals("NA") && !director.equals("na") && !director.equals("n/a") && !director.equals("N\\A") && !director.equals("N/A") && !director.equals("n/d"))
                    directors.add(director);
            }
        }
        return createMapListFromSet(Labels.Director,directors);
    }

    private static ArrayList<Map<String, Object>> getVODsFromDB() {
        HashSet<String> vodMenuWords = new HashSet<>();
        for (Database.Item i : dbExt.items(null)) {
            HashSet<String> mWords = getUniqueItemMetaWordsByKey(i.idx, "VodMenuDirect", "[\f/]");
            for (String mW : mWords)
                vodMenuWords.add(mW);
        }
        return createMapListFromSet(Labels.VOD,vodMenuWords);
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

    private static void insertRelationshipsWithoutProperties(Relationships rel, ArrayList<Link<Long>> links){
        LogHelper.INSTANCE.log(rel.name() + " list keszitese: ");
        int num = 0;
        for(Link<Long> l : links){
            batchInserter.createRelationship(l.startNode,l.endNode,rel,null);
            num++;
        }
        LogHelper.INSTANCE.log(rel.name() +" list keszitese KESZ: " + num);
    }

    private static ArrayList<Link<Long>> getSEENRelationships() {
        HashSet<Link<Long>> uniqueEvents = new HashSet<>(dbExt.numEvents());
        HashMap<Object,Long> userIDToIDPairs = IDToIDs.get(Labels.User);
        HashMap<Object,Long> itemIDToIDPairs = IDToIDs.get(Labels.Item);
        for (Database.Event e : dbExt.events(null)) {
            long userGraphDBID = userIDToIDPairs.get(dbExt.getUserId(e.uIdx));
            long itemGraphDBID = itemIDToIDPairs.get(dbExt.getItemId(e.iIdx));
            uniqueEvents.add(new Link(userGraphDBID,itemGraphDBID));
        }
        IDToIDs.remove(Labels.User);    //betöltve minden, ami hozzájuk kapcsolódik, ezért törlöm

        ArrayList<Link<Long>> events = new ArrayList<>(uniqueEvents.size());
        for(Link l : uniqueEvents){
            events.add(l);
        }

        return events;
    }

    private static ArrayList<Link<Long>> getACTSINRelationships() {
        ArrayList<Link<Long>> acts = new ArrayList<>(dbExt.numItems());
        HashMap<Object,Long> actorIDToIDPairs = IDToIDs.get(Labels.Actor);
        HashMap<Object,Long> itemIDToIDPairs = IDToIDs.get(Labels.Item);
        for (Database.Item i : dbExt.items(null)) {
            HashSet<String> actors = new HashSet<>();
            for (String actor : dbExt.getItemKeyValue(i.idx, "Actor").split("\f")) {
                if (!actor.equals("") && !actor.equals("NA") && !actor.equals("na") && !actor.equals("n/a") && !actor.equals("N\\A") && !actor.equals("N/A"))
                    actors.add(actor);
            }
            long itemGraphDBID = itemIDToIDPairs.get(dbExt.getItemId(i.idx));
            for(String s : actors){
                long actorGraphDBID = actorIDToIDPairs.get(s);
                acts.add(new Link(itemGraphDBID,actorGraphDBID));
            }

        }
        IDToIDs.remove(Labels.Actor);
        return acts;
    }

    private static ArrayList<Link<Long>> getDIRBYRelationships() {
        ArrayList<Link<Long>> directs = new ArrayList<>(dbExt.numItems() * 2);
        HashMap<Object,Long> directorIDToIDPairs = IDToIDs.get(Labels.Director);
        HashMap<Object,Long> itemIDToIDPairs = IDToIDs.get(Labels.Item);
        for (Database.Item i : dbExt.items(null)) {
            HashSet<String> directors = new HashSet<>();
            for (String director : dbExt.getItemKeyValue(i.idx, "Director").split("\f")) {
                if (!director.equals("") && !director.equals("NA") && !director.equals("na") && !director.equals("n/a") && !director.equals("N\\A") && !director.equals("N/A") && !director.equals("n/d"))
                    directors.add(director);
            }
            long itemGraphDBID = itemIDToIDPairs.get(dbExt.getItemId(i.idx));
            for(String s : directors){
                long directorGraphDBID = directorIDToIDPairs.get(s);
                directs.add(new Link(itemGraphDBID,directorGraphDBID));
            }
        }
        IDToIDs.remove(Labels.Director);
        return directs;
    }

    private static ArrayList<Link<Long>> getHASMETARelationships() {
        ArrayList<Link<Long>> meta = new ArrayList<>(dbExt.numItems() * 10);
        HashMap<Object,Long> vodIDToIDPairs = IDToIDs.get(Labels.VOD);
        HashMap<Object,Long> itemIDToIDPairs = IDToIDs.get(Labels.Item);
        for (Database.Item i : dbExt.items(null)) {
            HashSet<String> mWords = getUniqueItemMetaWordsByKey(i.idx, "VodMenuDirect", "[\f/]");

            long itemGraphDBID = itemIDToIDPairs.get(dbExt.getItemId(i.idx));
            for(String s : mWords){
                long vodGraphDBID = vodIDToIDPairs.get(s);
                meta.add(new Link(itemGraphDBID,vodGraphDBID));
            }
        }
        IDToIDs.remove(Labels.VOD);
        IDToIDs.remove(Labels.Item);
        return meta;
    }

    private static HashSet<String> getUniqueItemMetaWordsByKey(int iIdx, String key, String key_value_separator) {
        HashSet<String> itemMetaWords = new HashSet<>();
        String keyAll = dbExt.getItemKeyValue(iIdx, key);
        String[] values = keyAll.split(key_value_separator);

        for (String val : values) {
            if (!val.equals("") && val.length() > 2)
                itemMetaWords.add(val);
        }
        return itemMetaWords;
    }



}
