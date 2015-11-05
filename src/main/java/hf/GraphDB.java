package hf;

import gnu.trove.iterator.TObjectDoubleIterator;
import gnu.trove.iterator.TObjectIntIterator;
import gnu.trove.list.array.TDoubleArrayList;
import gnu.trove.map.hash.TObjectDoubleHashMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import onlab.core.Database;
import onlab.core.ExtendedDatabase;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.index.UniqueFactory;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.graphdb.traversal.*;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.kernel.impl.store.MetaDataStore;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by ufnagyi
 */
public class GraphDB {
    public GraphDatabaseService graphDBService = null;
    public Database db = null;
    public ExtendedDatabase dbExt = null;

    public enum Labels implements Label {
        Item("ItemID"), User("UserID"), Meta("word");
        private String property;

        Labels(String str) {
            this.property = str;
        }

        public String getPropertyName(){
            return property;
        }
    }

    public enum Relationships implements RelationshipType {
        SEEN("buy"), VOD("tag"), DIRECTED_BY("dir"), ACTS_IN("act"), ITEM_SIM("cfsim");
        private String property;

        Relationships(String str) {
            this.property = str;
        }

        public String getPropertyName(){
            return property;
        }
    }

    private static void registerShutdownHook( final GraphDatabaseService graphDb )
    {
        // Registers a shutdown hook for the Neo4j instance so that it
        // shuts down nicely when the VM exits (even if you "Ctrl-C" the
        // running application).
        Runtime.getRuntime().addShutdownHook( new Thread()
        {
            @Override
            public void run()
            {
                graphDb.shutdown();
            }
        } );
    }

    public void initDB(Database db, File dbFolder){
        this.db = db;
//        this.dbExt = (ExtendedDatabase) db;
        graphDBService = new GraphDatabaseFactory().newEmbeddedDatabase(dbFolder);
        registerShutdownHook(graphDBService);
    }


    public void buildDBFromImpressDB() {
        loadMetaWordsToGraphDB();
        loadItemsWithMetaWordsToGraphDB();
        loadUsersToGraphDB();
        createIndexes();
        loadEventRelationshipsToGraphDB();
        listGraphDBInfo();
    }

    private void createIndexes() {
        Transaction tx;

        //Cypherrel indexeles:

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
        if(!checkForIndex(schema.getIndexes(Labels.Meta),Labels.Meta.getPropertyName())) {
            schema.indexFor(Labels.Meta).on(Labels.Meta.getPropertyName()).create();
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
        UniqueFactory<Node> mWordFactory = createUniqueNodeFactory(Labels.Meta, graphDBService);

        //Metawordok feltöltése a gráfba
        Iterator<String> iterator = allUniqueMetaWords.iterator();
        int j = 0;
        while(iterator.hasNext()) {
            String mW = iterator.next();
            mWordFactory.getOrCreate(Labels.Meta.getPropertyName(),mW);
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
        UniqueFactory<Node> mWordFactory = createUniqueNodeFactory(Labels.Meta, graphDBService);

        //Itemek feltöltése a gráfba
        for (Database.Item i : db.items(null)) {
            int iIdx = i.idx;
            itemNode = itemFactory.getOrCreate(Labels.Item.getPropertyName(), iIdx);
            setItemProperty(itemNode,i);
            //Item mWord node-ok letrehoza
            HashSet<String> itemMWords = getUniqueItemMetaWordsByKey(iIdx, "VodMenuDirect", "[^\\p{L}0-9:_ ]");
            for (String mW : itemMWords) {
                metaWNode = mWordFactory.getOrCreate(Labels.Meta.getPropertyName(), mW);
                createUniqueRelationship(Relationships.VOD.getPropertyName(), iIdx + "_" + mW, Relationships.VOD, itemNode, metaWNode);
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

    public void computeItemToItemSims() {
        Transaction tx = graphDBService.beginTx();
            ArrayList<Node> nodeList = new ArrayList<>();

        TraversalDescription simNodeFinder = graphDBService.traversalDescription()
                .depthFirst()
                .relationships(Relationships.SEEN)
                .evaluator(Evaluators.atDepth(2));


        ResourceIterator<Node> itemNodes = graphDBService.findNodes(Labels.Item);
        while (itemNodes.hasNext()) {
            Node n = itemNodes.next();
//            if(Integer.parseInt((String)n.getProperty("id")) < 100) nodeList.add(n);
            nodeList.add(n);
        }

        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        System.out.println("Szamitas kezdese:" + dateFormat.format(Calendar.getInstance().getTimeInMillis()));
        int uncommittedChanges = 0;
        for(Node item : nodeList) {
            uncommittedChanges = uncommittedChanges + computeCosineSimilarity(item, simNodeFinder, Relationships.SEEN, Relationships.ITEM_SIM);
            if(uncommittedChanges > 1000) {
                System.out.println("Commit");
                uncommittedChanges = 0;
                tx.success();
                tx.close();
                tx = graphDBService.beginTx();
            }
        }
        tx.success();
        tx.close();


//        try {
//            PrintWriter out = new PrintWriter("similarity.txt");
//            TObjectDoubleIterator<Pair> iterator = itemToItemSimilarities.iterator();
//            i = 0;
//            while(iterator.hasNext()){
//                iterator.advance();
//                Pair p = iterator.key();
//                out.println(p.iIdx1 + "\t" + p.iIdx2 + "\t" + iterator.value());
//                i++;
//                if(i > 1000) {
//                    out.flush();
//                    i = 0;
//                }
//            }
//        } catch (FileNotFoundException e) {
//            e.printStackTrace();
//        }

    }

    public HashSet<Long> getNeighbors(Node node, Relationships rel){
        HashSet<Long> neighbors = new HashSet<>();
        for( Relationship relationship : node.getRelationships(rel)){
            neighbors.add(relationship.getOtherNode(node).getId());
        }
        return neighbors;
    }

    public int computeCosineSimilarity(Node nodeA, TraversalDescription description,
                                          Relationships existingRelType, Relationships newRelType){
        int changeCounter = 0;
        HashSet<Long> computedSimNodes = getNeighbors(nodeA,newRelType);   //a már kiszámolt item szomszédok
        TObjectIntHashMap<Node> friendNodes = new TObjectIntHashMap<>();
        TObjectIntHashMap<Long> suppABForAllB = new TObjectIntHashMap<>();

        int suppA = nodeA.getDegree(existingRelType);

        for(Path path : description.traverse(nodeA)) {
            Node friendNode = path.endNode();
            long id = friendNode.getId();
            if (!computedSimNodes.contains(id)) {                   //ha még nem lett kiszámolva a hasonlóságuk
                friendNodes.put(friendNode,friendNode.getDegree(existingRelType));      //suppB
                suppABForAllB.adjustOrPutValue(id,1,1);                    //suppAB növelés
            }
        }

        TObjectIntIterator<Node> friends = friendNodes.iterator();
        while(friends.hasNext()){
            friends.advance();
            Node friend = friends.key();
            int suppB = friends.value();
            double suppAB = (double) suppABForAllB.get(friend.getId());

            double sim = suppAB / (suppA * suppB);
//            System.out.println("A: " + suppA + " B: " + suppB + " AB: " + suppAB + " sim: " + sim);
            Relationship newRel = nodeA.createRelationshipTo(friend,newRelType);
            newRel.setProperty(newRelType.getPropertyName(),sim);
            changeCounter++;
//            System.out.println(sim);

        }
//        System.out.println("Kész: " + nodeA.getProperty("id"));
        return changeCounter;
    }

}
