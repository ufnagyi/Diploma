package hf;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Ordering;
import gnu.trove.map.hash.TLongObjectHashMap;
import onlab.core.Database;
import onlab.core.ExtendedDatabase;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.tooling.GlobalGraphOperations;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class GraphDB {
    public GraphDatabaseService graphDBService = null;
    public Database db = null;
    public ExtendedDatabase dbExt = null;
    private File dbFolder;
    private boolean inited = false;

    public GraphDB(Database db, String newDBFolder) {
        this.db = db;
        this.dbExt = (ExtendedDatabase) db;
        setDbFolder(newDBFolder);
    }

    public GraphDB(String oldDBFolder){
        setDbFolder(oldDBFolder);
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
        graphDBService = new GraphDatabaseFactory().newEmbeddedDatabase(dbFolder);
        registerShutdownHook(graphDBService);
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

    public HashSet<Long> getAllNeighborIDsByRel(Node node, Relationships rel){
        HashSet<Long> neighbors = new HashSet<>();
        for( Relationship relationship : node.getRelationships(rel)){
            neighbors.add(relationship.getOtherNode(node).getId());
        }
        return neighbors;
    }

    public HashSet<Node> getAllNeighborsByRel(Node node, Relationships rel){
        HashSet<Node> neighbors = new HashSet<>();
        for( Relationship relationship : node.getRelationships(rel)){
            neighbors.add(relationship.getOtherNode(node));
        }
        return neighbors;
    }


    public HashMap<Node,Double> getAllNeighborsBySim(Node node, Similarities sim){
        HashMap<Node, Double> neighbors = new HashMap<>();
        for (Relationship relationship : node.getRelationships(sim)) {
            neighbors.put(relationship.getOtherNode(node), (Double) relationship.getProperty(sim.getPropertyName()));
        }
        return neighbors;
    }

    public HashMap<Long,Double> getAllNeighborIDsBySim(Node node, Similarities sim){
        HashMap<Long, Double> neighbors = new HashMap<>();
        for (Relationship relationship : node.getRelationships(sim)) {
            neighbors.put(relationship.getOtherNode(node).getId(), (Double) relationship.getProperty(sim.getPropertyName()));
        }
        return neighbors;
    }

    /**
     *
     * @param l A Similarity kiindulo node-janak cimkeje
     * @param sim milyen Similarityt keresunk
     * @return
     */
    public HashSet<SimLink> getAllSimilarities(Labels l,Similarities sim){
        HashSet<SimLink> similarities = new HashSet<>();
        Transaction tx = this.startTransaction();
        Iterable<Relationship> allRelationships = GlobalGraphOperations.at(graphDBService).getAllRelationships();
        for(Relationship relationship : allRelationships)
        {
            if (relationship.isType(sim))
            {
                System.out.println(relationship.getStartNode().getProperty(l.getIDName()).getClass().getTypeName());
            }
        }
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
    public void batchInsertSimilarities(HashSet<SimLink> sims, Similarities simLabel) {
        this.shutDownDB();
        this.inited = false;
        BatchInserter batchInserter = null;
        try {
            batchInserter = BatchInserters.inserter(this.dbFolder.getAbsoluteFile());
            for(SimLink<Long> s : sims){
                Map<String, Object> simProperty = ImmutableMap.of(simLabel.getPropertyName(), s.similarity);
                batchInserter.createRelationship((long) s.startNode, (long)s.endNode,simLabel,simProperty);
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


}
