package hf.GraphUtils;

import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Ordering;
import gnu.trove.map.hash.TIntDoubleHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.map.hash.TLongDoubleHashMap;
import onlab.core.Database;
import onlab.core.ExtendedDatabase;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.tooling.GlobalGraphOperations;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.stream.StreamSupport;

public class GraphDB {
    public GraphDatabaseService graphDBService = null;
    public Database db = null;
    public ExtendedDatabase dbExt = null;
    private File dbFolder;
    private boolean inited = false;
    public HashSet<String> stopWords;
    public boolean uniqueEvents;
    public boolean filterStopWords;
    public long initialDBSize;

    public GraphDB(Database db, String DBFolder, boolean uniqueEvents, boolean filterStopWords) {
        this.db = db;
        this.dbExt = (ExtendedDatabase) db;
        setDbFolder(DBFolder);
        this.uniqueEvents = uniqueEvents;
        this.filterStopWords = filterStopWords;
        this.stopWords = (filterStopWords) ? GraphDBBuilder.loadStopWordsFromFile() : null;
        this.initialDBSize = this.getDBSize();
    }

    public void printParameters(){
        LogHelper.INSTANCE.logToFile("GraphDB parameters:");
        LogHelper.INSTANCE.logToFile("Event list: " + (this.uniqueEvents ? "unique" : "all"));
        LogHelper.INSTANCE.logToFile("Stopwords: " + (filterStopWords ? "filtered" : "unfiltered"));
        LogHelper.INSTANCE.logToFile("Database size: " + this.getDBSize() + " MB");
    }

    private static void registerShutdownHook(final GraphDatabaseService graphDb) {
        // Registers a shutdown hook for the Neo4j instance so that it
        // shuts down nicely when the VM exits (even if you "Ctrl-C" the
        // running application).
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                graphDb.shutdown();
            }
        });
    }

    public boolean isInited() {
        return inited;
    }

    public File getDbFolder() {
        return dbFolder;
    }

    public void setDbFolder(String dbFolder) {
        this.dbFolder = new File(dbFolder);
    }

    public void initDB(){
        if(!inited) {
            graphDBService = new GraphDatabaseFactory().newEmbeddedDatabase(dbFolder);
            registerShutdownHook(graphDBService);
        }
        inited = true;
    }

    public Transaction startTransaction(){
        return this.graphDBService.beginTx();
    }

    public void endTransaction(Transaction tx){
        tx.success();
        tx.close();
    }

    public void shutDownDB(){
        if (inited)
            graphDBService.shutdown();
        inited = false;
    }

    public long getDBSize(){
        long MB = 1024*1024;
        Iterable<File> files = com.google.common.io.Files.fileTreeTraverser()
                .breadthFirstTraversal(this.getDbFolder());
        long size = StreamSupport.stream(files.spliterator(), false)
                .filter(f -> f.isFile())
                .mapToLong(File::length).sum();
        return size / MB;
    }

    public void listGraphDBInfo(){
        System.out.println("--------------------------------------");
        System.out.println("Adatbazis informaciok:");
        Transaction tx = this.startTransaction();
        Schema schema = graphDBService.schema();
        System.out.println("---Label schema indexek:");
        for (IndexDefinition definition : schema.getIndexes())
            System.out.println(definition.toString());
        System.out.println("---Legacy indexek:");
        System.out.println("------Node indexek:");
        for (String s : graphDBService.index().nodeIndexNames())
            System.out.println(s);
        System.out.println("------Relationship indexek:");
        for (String s : graphDBService.index().relationshipIndexNames())
            System.out.println(s);
        Result r = graphDBService.execute("MATCH (n:Item) RETURN COUNT(n)");
        System.out.print("Itemek szama: ");
        while(r.hasNext())
            System.out.println(r.next().values().toString());
        r = graphDBService.execute("MATCH (n:User) RETURN COUNT(n)");
        System.out.print("Userek szama: ");
        while(r.hasNext())
            System.out.println(r.next().values().toString());
        r = graphDBService.execute("MATCH (n:Meta) RETURN COUNT(n)");
        System.out.print("Metawordok szama: ");
        while(r.hasNext())
            System.out.println(r.next().values().toString());
        r = graphDBService.execute("MATCH (n:Item)-[r]->(w:Meta) RETURN TYPE(r), COUNT(*)");
        System.out.println("Item-Metaword kapcsolatok szama: ");
        while(r.hasNext())
            System.out.println(r.next().toString());
        r = graphDBService.execute("MATCH (n:User)-[r]->(w:Item) RETURN TYPE(r), COUNT(*)");
        System.out.println("Item-Metaword kapcsolatok szama: ");
        while(r.hasNext())
            System.out.println(r.next().toString());
        r = graphDBService.execute("MATCH (n:Item)-[r]->(w:Meta) RETURN n.itemID, n.name, COUNT(*) AS CNT ORDER BY CNT DESC LIMIT 5");
        System.out.print("Item-Metaword legtobb kapcsolatu 5 Item: ");
        while(r.hasNext())
            System.out.println(r.next().toString());
        this.endTransaction(tx);
    }

    /**
     * Adott node osszes szomszedjanak DB-beli(!) ID-ja rel menten
     */
    public static HashSet<Long> getAllNeighborDBIDsByRel(Node node, Relationships rel){
        HashSet<Long> neighbors = new HashSet<>();
        for( Relationship relationship : node.getRelationships(rel)){
            neighbors.add(relationship.getOtherNode(node).getId());
        }
        return neighbors;
    }

    public static Iterable<Relationship> getDistinctRelationships(Node node, Relationships rel){
        ArrayList<Relationship> distinctRelationships = new ArrayList<>();
        Iterator<Relationship> iterator = node.getRelationships(rel).iterator();
        HashSet<Long> nodes = new HashSet<>();
        while(iterator.hasNext()){
            Relationship rel_ = iterator.next();
            long otherNode = rel_.getOtherNode(node).getId();
            if(!nodes.contains(otherNode)){
                nodes.add(otherNode);
                distinctRelationships.add(rel_);
            }
        }
        return distinctRelationships;
    }


    /**
     * Adott node osszes szomszedjanak ID-ja rel menten
     * @param label A node cimkeje
     * @param node A node
     * @param rel Relaciotipus, ami menten keresunk
     * @return
     */
    public static HashSet<Integer> getAllNeighborIDsByRel(Labels label, Node node, Relationships rel){
        HashSet<Integer> neighbors = new HashSet<>();
        for( Relationship relationship : node.getRelationships(rel)){
            neighbors.add((int)relationship.getOtherNode(node).getProperty(label.getIDName()));
        }
        return neighbors;
    }

    public static int getDistinctDegree(Node node, Relationships rel){
        return GraphDB.getAllNeighborDBIDsByRel(node,rel).size();
    }

    public static HashMap<Node,Double> getAllNeighborsBySim(Node node, Similarities sim){
        HashMap<Node, Double> neighbors = new HashMap<>();
        for (Relationship relationship : node.getRelationships(sim)) {
            neighbors.put(relationship.getOtherNode(node), (Double) relationship.getProperty(sim.getPropertyName()));
        }
        return neighbors;
    }

    public static HashMap<Long,Double> getAllNeighborIDsBySim(Node node, Similarities sim){
        HashMap<Long, Double> neighbors = new HashMap<>();
        for (Relationship relationship : node.getRelationships(sim)) {
            neighbors.put(relationship.getOtherNode(node).getId(), (Double) relationship.getProperty(sim.getPropertyName()));
        }
        return neighbors;
    }

    public void computeAndUploadIDFValues(Labels l, Relationships rel){
        LogHelper.INSTANCE.logToFileT(l.name() + " node-ok IDF értékeinek számitása és feltöltése:");
        Transaction tx = this.startTransaction();
        double numOfItems = (double) IteratorUtil.count(graphDBService.findNodes(Labels.Item));
        ArrayList<Node> nodeList = this.getNodesByLabel(l);
        for(Node n : nodeList){
            n.setProperty("idf", Math.log(numOfItems / n.getDegree(rel)));
        }
        this.endTransaction(tx);
        LogHelper.INSTANCE.logToFileT(nodeList.size() + " érték feltöltve!");
    }

    /**
     * Osszes meta node IDF erteke
     * @param labels Meta node tipus
     * @return
     */
    public TLongDoubleHashMap getMetaIDFValues(Labels[] labels){
        Transaction tx = this.startTransaction();
        TLongDoubleHashMap wordIDFs = new TLongDoubleHashMap();
        for(Labels l : labels){
            ArrayList<Node> nodeList = this.getNodesByLabel(l);
            for(Node n : nodeList)
                wordIDFs.put(n.getId(), (double) n.getProperty("idf"));
        }
        this.endTransaction(tx);
        return wordIDFs;
    }

    public HashBiMap<String, Long> getMetaIDs(Labels[] labels){
        Transaction tx = this.startTransaction();
        HashBiMap<String, Long> wordIDs = HashBiMap.create();
//        TObjectLongHashMap wordIDs = new TObjectLongHashMap();
        int i = 0;
        for(Labels l : labels){
            ArrayList<Node> nodeList = this.getNodesByLabel(l);
            for(Node n : nodeList)
                wordIDs.put(i + "" + n.getProperty(l.getPropertyName()),n.getId());
            i++;
        }
        this.endTransaction(tx);
        return wordIDs;
    }

    public void deleteSimilaritiesByType(Similarities sim){
        LogHelper.INSTANCE.logToFileT(sim.name() + " hasonlóságok törlése!");
        Transaction tx = this.startTransaction();
        long deletedSims = 0;
        long deletedSims2 = 0;
        Iterable<Relationship> allRelationships = GlobalGraphOperations.at(graphDBService).getAllRelationships();
        for (Relationship relationship : allRelationships) {
            if (relationship.isType(sim)) {
                relationship.delete();
                deletedSims2++;
            }
            if(deletedSims2 > 100000){
                this.endTransaction(tx);
                tx = this.startTransaction();
                deletedSims += deletedSims2;
                deletedSims2 = 0;
                System.out.println(deletedSims);
            }
        }
        this.endTransaction(tx);
        deletedSims += deletedSims2;
        LogHelper.INSTANCE.logToFileT("Törölt hasonlóságok száma: " + deletedSims + "!");
    }

    /**
     * Node-ok kozti hasonlosagok
     * @param l A Similarity kiindulo node-janak cimkeje
     * @param sim milyen Similarityt keresunk
     * @return
     */
    public TIntObjectHashMap<TIntDoubleHashMap> getAllSimilaritiesBySim(Labels l, Similarities sim) {
        TIntObjectHashMap<TIntDoubleHashMap> similarities = new TIntObjectHashMap<>();
        int simNUM = 0;
        Transaction tx = this.startTransaction();
        Iterable<Relationship> allRelationships = GlobalGraphOperations.at(graphDBService).getAllRelationships();
        for (Relationship relationship : allRelationships) {
            if (relationship.isType(sim)) {
                int startNodeID = (int) relationship.getStartNode().getProperty(l.getIDName());
                int endNodeID = (int) relationship.getEndNode().getProperty(l.getIDName());
                double similarity = (double) relationship.getProperty(sim.getPropertyName());
                Link<Integer> link = new Link<>(startNodeID,endNodeID);
                if (similarities.containsKey(link.startNode))
                    similarities.get(link.startNode).put(link.endNode, similarity);
                else {
                    TIntDoubleHashMap asd = new TIntDoubleHashMap();
                    asd.put(link.endNode, similarity);
                    similarities.put(link.startNode, asd);
                }
                simNUM++;
            }
        }
        this.endTransaction(tx);
        System.out.println(simNUM);
        return similarities;
    }


    /** Visszaadja a topN szomszed node-ot a parameterul kapott hasonlosagi eltipus alapjan
     * @param node StartNode
     * @param sim  Mely hasonlosagot vizsgalja
     * @param topN N leghasonlobb szomszed! Ha N = -1 -> osszes szomszed
     * @return Szomszed es a hozza tartozo hasonlosag.
     */
    public Map<Node, Double> getTopNNeighborsAndSims(Node node, Similarities sim, int topN) {
        Transaction tx = this.startTransaction();
        HashMap<Node, Double> neighbors = getAllNeighborsBySim(node, sim);
        Map<Node, Double> result = new LinkedHashMap<>();

        ArrayList<Map.Entry<Node, Double>> list = new ArrayList<>(neighbors.entrySet());
        List<Map.Entry<Node, Double>> entries;
        if (topN != -1) {
            entries = Ordering.from(this.getComparator()).greatestOf(list, topN);
            for (Map.Entry<Node, Double> entry : entries) {
                result.put(entry.getKey(), entry.getValue());
            }
        } else {
            Collections.sort(list, this.getComparator().reversed());
            for (Map.Entry<Node, Double> entry : list) {
                result.put(entry.getKey(), entry.getValue());
            }
        }
        this.endTransaction(tx);
        return result;
    }

    public Comparator<Map.Entry<Node, Double>> getComparator() {
        return (o1, o2) -> (o1.getValue()).compareTo(o2.getValue());
    }

    /**
     * Adott l labellel rendelkezo osszes node-ot visszaadja
     * @param l A Label tipus
     * @return Node ArrayList
     */
    public ArrayList<Node> getNodesByLabel(Labels l){
        Transaction tx = this.startTransaction();
        ArrayList<Node> nodesByLabel = new ArrayList<>();
        ResourceIterator<Node> nodes = this.graphDBService.findNodes(l);
        while(nodes.hasNext()){
            nodesByLabel.add(nodes.next());
        }
        this.endTransaction(tx);
        return nodesByLabel;
    }

    /**
     * Adott l labellel rendelkezo osszes node-ot visszaadja
     * @param l A Label tipus
     * @return Node ArrayList
     */
    public long getMinNodeIDByLabel(Labels l){
        Transaction tx = this.startTransaction();
        ArrayList<Long> nodeIDsByLabel = new ArrayList<>();
        ResourceIterator<Node> nodes = this.graphDBService.findNodes(l);
        while(nodes.hasNext()){
            nodeIDsByLabel.add(nodes.next().getId());
        }
        this.endTransaction(tx);
        return Collections.min(nodeIDsByLabel);
    }

    /**
     * Tranzakció és ellenőrzés nélküli hasonlósági kapcsolatok gráfdb-be szúrása
     * @param sims Kiszamolt hasonlosagok
     * @param simLabel Hasonlosagok relaciotipusa
     */
    public void batchInsertSimilarities(HashSet<SimLink<Long>> sims, Similarities simLabel) {
        this.shutDownDB();
        this.inited = false;
        BatchInserter batchInserter = null;
        try {
            batchInserter = BatchInserters.inserter(this.dbFolder.getAbsoluteFile());
            for(SimLink<Long> s : sims){
                Map<String, Object> simProperty = ImmutableMap.of(simLabel.getPropertyName(), s.similarity);
                batchInserter.createRelationship(s.startNode, s.endNode,simLabel,simProperty);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if ( batchInserter != null )
            {
                batchInserter.shutdown();
            }
        }
    }

    /**
     * Adott node elszama a relaciotipusok menten
     * @return
     */
    public static int getSumOfDegreesByRelationships(Node node, Relationships[] relationships){
        int degree = 0;
        for(Relationships r : relationships){
            degree += node.getDegree(r);
        }
        return degree;
    }

    public TIntObjectHashMap<HashSet<Integer>> getAllUserItems() {
        Transaction tx = this.startTransaction();
        ArrayList<Node> users = this.getNodesByLabel(Labels.User);
        int count = users.size();
        int percent1 = count / 100;
        int percent = 0;
        int percent_act = 1;
        TIntObjectHashMap<HashSet<Integer>> userItems = new TIntObjectHashMap<>(count);
        count = 0;
        for(Node user : users){
            int userID = (int) user.getProperty(Labels.User.getIDName());
            userItems.put(userID, GraphDB.getAllNeighborIDsByRel(Labels.Item, user, Relationships.SEEN));
            if(++count > percent){
                percent = percent + percent1;
                System.out.print(percent_act + "% ");
                percent_act++;
            }
        }
        this.endTransaction(tx);
        System.out.println();
        return userItems;
    }
}
