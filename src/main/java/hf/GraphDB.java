package hf;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Ordering;
import onlab.core.Database;
import onlab.core.ExtendedDatabase;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.index.UniqueFactory;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
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

    public void shutDownDB(){
        if (inited)
            graphDBService.shutdown();
        inited = false;
    }


    public void buildDBFromImpressDB() {
        if(this.db == null) {
            try {
                throw new Exception("ImpressDB nincs megadva a gráfépítéshez!");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        System.out.println("GraphDB epites kezdese:" + dateFormat.format(Calendar.getInstance().getTimeInMillis()));
        loadMetaWordsToGraphDB();
        loadItemsWithMetaWordsToGraphDB();
        loadUsersToGraphDB();
        createIndexes();
        loadEventRelationshipsToGraphDB();
        System.out.println("GraphDB epites kesz!:" + dateFormat.format(Calendar.getInstance().getTimeInMillis()));
        listGraphDBInfo();
    }

    private void createIndexes() {
        Transaction tx;

        //region Cypherrel indexeles:

//        tx = graphDBService.beginTx();
//        graphDBService.execute("CREATE INDEX ON :Item(itemID)");
//        tx.success();
//        tx.close();
//
//        tx = graphDBService.beginTx();
//        graphDBService.execute("CREATE INDEX ON :User(userID)");
//        tx.success();
//        tx.close();
//
//        tx = graphDBService.beginTx();
//        graphDBService.execute("CREATE INDEX ON :Meta(mword)");
//        tx.success();
//        tx.close();

        //endregion

        //Javaval indexeles

        tx = graphDBService.beginTx();
        Schema schema = graphDBService.schema();
        //item és user indexelés ID szerint
        if(!checkForIndex(schema.getIndexes(Labels.Item),Labels.Item.getPropertyName())) {
            schema.indexFor(Labels.Item).on(Labels.Item.getPropertyName()).create();
            tx.success();
            tx.close();
            tx = graphDBService.beginTx();
        }
        if(!checkForIndex(schema.getIndexes(Labels.User),Labels.User.getPropertyName())) {
            schema.indexFor(Labels.User).on(Labels.User.getPropertyName()).create();
            tx.success();
            tx.close();
            tx = graphDBService.beginTx();
        }
        if(!checkForIndex(schema.getIndexes(Labels.VOD),Labels.VOD.getPropertyName())) {
            schema.indexFor(Labels.VOD).on(Labels.VOD.getPropertyName()).create();
            tx.success();
            tx.close();
            tx = graphDBService.beginTx();
        }
        for (IndexDefinition definition : schema.getIndexes())
            System.out.println(definition.toString());
        tx.success();
        tx.close();
    }

    private boolean checkForIndex(Iterable<IndexDefinition> indexes, String propertyName){
        for(IndexDefinition index : indexes) {
            for (String key: index.getPropertyKeys()) {
                if (key.equals(propertyName)) {
                    return true; // index for given label and property exists
                }
            }
        }
        return false;
    }

    private void loadMetaWordsToGraphDB() {
        int mwCNT = 0;

        HashSet<String> allUniqueMetaWords = new HashSet<>(100);

        for (Database.Item i : db.items(null)) {
            HashSet<String> itemMWords = getUniqueItemMetaWordsByKey(i.idx,"VodMenuDirect", "[^\\p{L}0-9:_. ]");
            for (String mW : itemMWords) {
                if (!allUniqueMetaWords.contains(mW)) {
                    allUniqueMetaWords.add(mW);
                    mwCNT++;
                }
            }
        }
        System.out.println(mwCNT);

        Transaction tx = graphDBService.beginTx();
        UniqueFactory<Node> mWordFactory = createUniqueNodeFactory(Labels.VOD, graphDBService);

        //Metawordok feltöltése a gráfba
        Iterator<String> iterator = allUniqueMetaWords.iterator();
        int j = 0;
        while(iterator.hasNext()) {
            String mW = iterator.next();
            mWordFactory.getOrCreate(Labels.VOD.getPropertyName(),mW);
            j++;
            if (j % 100 == 0) {
                tx.success();
                tx.close();
                tx = graphDBService.beginTx();
                System.out.println(j);
            }
        }
        tx.success();
        tx.close();

        System.out.println("A metaword node-ok feltoltese sikerult!");
    }


    //TODO: Doksiban leírni: numofItems << numofUsers --> event bejarasnal itemek legyenek a kulso ciklusban!
    //--> kevesebb node lekérdezés!
    //teljes feltöltési idő 32 perc!
    private void loadEventRelationshipsToGraphDB() {
        int numOfItems = 0;
        int numOfEvents = 0;
        Node userNode;
        Node itemNode;
        Transaction tx = graphDBService.beginTx();

        for (Database.Item i : db.items(null)) {
            itemNode = graphDBService.findNode(Labels.Item, Labels.Item.getPropertyName(), i.idx);
            if(itemNode == null) System.out.println("Item " + i.name + " | ID: " + i.idx + " nincs a grafban!");

            for (Database.Event e : db.itemEvents(i,null)){
                userNode = graphDBService.findNode(Labels.User,Labels.User.getPropertyName(),e.uIdx);
                createUniqueRelationship(Relationships.SEEN.getPropertyName(),e.uIdx + "_" + i.idx,Relationships.SEEN,userNode,itemNode);
                numOfEvents++;
                if (numOfEvents % 200 == 0) {
                    tx.success();
                    tx.close();
                    tx = graphDBService.beginTx();
                    if(numOfEvents % 1000 == 0)
                        System.out.println(numOfEvents);
                }
            }

            numOfItems++;
            if (numOfItems % 100 == 0) {
                System.out.println("Item:  " + numOfItems);
            }
        }
        tx.success();
        tx.close();

        System.out.println("A User-Item kapcsolatok feltoltese sikerult!");
    }

    private void loadUsersToGraphDB() {
        int j = 0;
        Transaction tx = graphDBService.beginTx();
        UniqueFactory<Node> userFactory = createUniqueNodeFactory(Labels.User, graphDBService);
        //Userek feltöltése a gráfba
        for (Database.User u : db.users(null)) {
            userFactory.getOrCreate(Labels.User.getPropertyName(), u.idx);
            j++;
            if (j % 1000 == 0) {
                tx.success();
                tx.close();
                tx = graphDBService.beginTx();
                System.out.println(j);
            }
        }
        tx.success();
        tx.close();

        System.out.println("A userek feltoltese sikerult!");
    }

    private void loadItemsWithMetaWordsToGraphDB() {
        int j = 0;
        Node itemNode;
        Node metaWNode;
        Transaction tx = graphDBService.beginTx();

        UniqueFactory<Node> itemFactory = createUniqueNodeFactory(Labels.Item, graphDBService);
        UniqueFactory<Node> mWordFactory = createUniqueNodeFactory(Labels.VOD, graphDBService);

        //Itemek feltöltése a gráfba
        for (Database.Item i : db.items(null)) {
            int iIdx = i.idx;
            itemNode = itemFactory.getOrCreate(Labels.Item.getPropertyName(), iIdx);
            setItemProperty(itemNode,i);
            //Item mWord node-ok letrehoza
            HashSet<String> itemMWords = getUniqueItemMetaWordsByKey(iIdx, "VodMenuDirect", "[^\\p{L}0-9:_ ]");
            for (String mW : itemMWords) {
                metaWNode = mWordFactory.getOrCreate(Labels.VOD.getPropertyName(), mW);
                createUniqueRelationship(Relationships.HAS_META.getPropertyName(), iIdx + "_" + mW, Relationships.HAS_META, itemNode, metaWNode);
            }
            j++;
            if (j % 100 == 0) {
                tx.success();
                tx.close();
                tx = graphDBService.beginTx();
                System.out.println(j);
            }
        }
        tx.success();
        tx.close();

        System.out.println("Az itemek feltoltese sikerult!");
    }

    private UniqueFactory<Node> createUniqueNodeFactory(final Labels nodeType, GraphDatabaseService graphDb) {
        return new UniqueFactory.UniqueNodeFactory(graphDb, nodeType.name()) {
            @Override
            protected void initialize(Node createdNode, Map<String, Object> properties) {
                createdNode.addLabel(nodeType);
                createdNode.setProperty(nodeType.getPropertyName(),properties.get(nodeType.getPropertyName()));
            }
        };
    }

    private Relationship createUniqueRelationship(String indexableKey, final String indexableValue,
                                                  final RelationshipType type, final Node start, final Node end) {

        UniqueFactory<Relationship> factory = new UniqueFactory.UniqueRelationshipFactory(graphDBService, indexableKey) {
            @Override
            protected Relationship create(Map<String, Object> properties) {
                return start.createRelationshipTo(end, type);
            }
        };
        return factory.getOrCreate(indexableKey, indexableValue);
    }
    private HashSet<String> getUniqueItemMetaWordsByKey(int iIdx, String key, String key_value_separator){
        HashSet<String> itemMetaWords = new HashSet<>();
        String keyAll = dbExt.getItemKeyValue(iIdx, key);
        String[] values = keyAll.split(key_value_separator);

        for (String val : values) {
            if (val.equals("") || val.length() < 3)
                continue;
            itemMetaWords.add(val.toLowerCase());
        }
        return itemMetaWords;
    }

    private void setItemProperty(Node node, Database.Item i){
        node.setProperty(Labels.Item.getPropertyName(),i.name);
    }

    public void listGraphDBInfo(){
        System.out.println("--------------------------------------");
        System.out.println("Adatbazis informaciok:");
        Transaction tx = graphDBService.beginTx();
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
        tx.success();
        tx.close();
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


    /** Visszaadja a topN szomszed node-ot a parameterul kapott hasonlosagi eltipus alapjan
     * @param node StartNode
     * @param sim  Mely hasonlosagot vizsgalja
     * @param topN N leghasonlobb szomszed! Ha N = -1 -> osszes szomszed
     * @return Szomszed es a hozza tartozo hasonlosag.
     */
    public Map<Node, Double> getTopNNeighborsAndSims(Node node, Similarities sim, int topN) {
        Transaction tx = graphDBService.beginTx();
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
        tx.success();
        tx.close();
        return result;
    }

    public Comparator<Map.Entry<Node, Double>> getComparator() {
        return (o1, o2) -> (o1.getValue()).compareTo(o2.getValue());
    }

    /**
     * Adott l labellel rendelkezo osszes node-ot visszaadja
     * @param l A Label tipus
     * @return
     */
    public ArrayList<Node> getNodesByLabel(Labels l){
        Transaction tx = graphDBService.beginTx();
        ArrayList<Node> nodesByLabel = new ArrayList<>();
        ResourceIterator<Node> nodes = this.graphDBService.findNodes(l);
        while(nodes.hasNext()){
            nodesByLabel.add(nodes.next());
        }
        tx.success();
        tx.close();
        return nodesByLabel;
    }

    /**
     * Tranzakció és ellenőrzés nélküli hasonlósági kapcsolatok gráfdb-be szúrása
     * @param sims Kiszamolt hasonlosagok
     * @param simLabel Hasonlosagok relaciotipusa
     */
    public void batchInsertSimilarities(HashSet<SimLink> sims, Similarities simLabel) {
        this.shutDownDB();
        BatchInserter batchInserter = null;
        try {
            batchInserter = BatchInserters.inserter(this.dbFolder.getAbsoluteFile());
            for(SimLink s : sims){
                Map<String, Object> simProperty = ImmutableMap.of(simLabel.getPropertyName(), s.similarity);
                batchInserter.createRelationship(s.startNode,s.endNode,simLabel,simProperty);
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
