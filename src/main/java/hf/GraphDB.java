package hf;

import onlab.core.Database;
import onlab.core.ExtendedDatabase;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.UniqueFactory;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by ufnagyi
 */
public class GraphDB {
    public GraphDatabaseService graphDB;
    public Database db = null;
    public ExtendedDatabase dbExt = null;

    public enum Labels implements Label {
        Item("itemID"), User("userID"), Meta("mword");
        private String property;

        private Labels(String str) {
            this.property = str;
        }

        public String getPropertyName(){
            return property;
        }
    }

    public enum Relationships implements RelationshipType {
        SEEN("buy"), HAS_META("tag");
        private String property;

        private Relationships(String str) {
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

    public void buildDB(Database db, String dbFolder) {
        this.db = db;
        this.dbExt = (ExtendedDatabase) db;

        graphDB = new GraphDatabaseFactory().newEmbeddedDatabase(dbFolder);
        registerShutdownHook(graphDB);

        //loadMetaWordsToGraphDB();
//        loadItemsWithMetaWordsToGraphDB();
//        loadUsersToGraphDB();
//        createIndexes();
//        listGraphDBInfo();

        loadEventRelationshipsToGraphDB();

    }

    private void createIndexes() {
        Transaction tx;

        //Cypherrel indexeles:

//        tx = graphDB.beginTx();
//        graphDB.execute("CREATE INDEX ON :Item(itemID)");
//        tx.success();
//        tx.close();
//
//        tx = graphDB.beginTx();
//        graphDB.execute("CREATE INDEX ON :User(userID)");
//        tx.success();
//        tx.close();
//
//        tx = graphDB.beginTx();
//        graphDB.execute("CREATE INDEX ON :Meta(mword)");
//        tx.success();
//        tx.close();

        //Javaval indexeles

        tx = graphDB.beginTx();
        Schema schema = graphDB.schema();
        //item és user indexelés ID szerint
        if(!checkForIndex(schema.getIndexes(Labels.Item),Labels.Item.getPropertyName())) {
            schema.indexFor(Labels.Item).on(Labels.Item.getPropertyName()).create();
            tx.success();
            tx.close();
            tx = graphDB.beginTx();
        }
        if(!checkForIndex(schema.getIndexes(Labels.User),Labels.User.getPropertyName())) {
            schema.indexFor(Labels.User).on(Labels.User.getPropertyName()).create();
            tx.success();
            tx.close();
            tx = graphDB.beginTx();
        }
        if(!checkForIndex(schema.getIndexes(Labels.Meta),Labels.Meta.getPropertyName())) {
            schema.indexFor(Labels.Meta).on(Labels.Meta.getPropertyName()).create();
            tx.success();
            tx.close();
            tx = graphDB.beginTx();
        }
        for (IndexDefinition definition : schema.getIndexes())
            System.out.println(definition.toString());
        tx.success();
        tx.close();
    }

    public boolean checkForIndex(Iterable<IndexDefinition> indexes, String propertyName){
        for(IndexDefinition index : indexes) {
            for (String key: index.getPropertyKeys()) {
                if (key.equals(propertyName)) {
                    return true; // index for given label and property exists
                }
            }
        }
        return false;
    }

    public void loadMetaWordsToGraphDB() {
        int mwCNT = 0;

        HashSet<String> allUniqueMetaWords = new HashSet<>(100);

        for (Database.Item i : db.items(null)) {
            HashSet<String> itemMWords = getUniqueItemMetaWordsByKey(i.idx,"VodMenuDirect", "[^\\p{L}0-9:_ ]");
            for (String mW : itemMWords) {
                if (!allUniqueMetaWords.contains(mW)) {
                    allUniqueMetaWords.add(mW);
                    mwCNT++;
                }
            }
        }
        System.out.println(mwCNT);

        Transaction tx = graphDB.beginTx();
        UniqueFactory<Node> mWordFactory = createUniqueNodeFactory(Labels.Meta,graphDB);

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
                tx = graphDB.beginTx();
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
        Transaction tx = graphDB.beginTx();

        for (Database.Item i : db.items(null)) {
            itemNode = graphDB.findNode(Labels.Item, Labels.Item.getPropertyName(), i.idx);
            if(itemNode == null) System.out.println("Item " + i.name + " | ID: " + i.idx + " nincs a grafban!");

            for (Database.Event e : db.itemEvents(i,null)){
                userNode = graphDB.findNode(Labels.User,Labels.User.getPropertyName(),e.uIdx);
                createUniqueRelationship(Relationships.SEEN.getPropertyName(),e.uIdx + "_" + i.idx,Relationships.SEEN,userNode,itemNode);
                numOfEvents++;
                if (numOfEvents % 200 == 0) {
                    tx.success();
                    tx.close();
                    tx = graphDB.beginTx();
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
        Transaction tx = graphDB.beginTx();
        UniqueFactory<Node> userFactory = createUniqueNodeFactory(Labels.User,graphDB);
        //Userek feltöltése a gráfba
        for (Database.User u : db.users(null)) {
            userFactory.getOrCreate(Labels.User.getPropertyName(), u.idx);
            j++;
            if (j % 1000 == 0) {
                tx.success();
                tx.close();
                tx = graphDB.beginTx();
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
        Transaction tx = graphDB.beginTx();

        UniqueFactory<Node> itemFactory = createUniqueNodeFactory(Labels.Item,graphDB);
        UniqueFactory<Node> mWordFactory = createUniqueNodeFactory(Labels.Meta,graphDB);

        //Itemek feltöltése a gráfba
        for (Database.Item i : db.items(null)) {
            int iIdx = i.idx;
            itemNode = itemFactory.getOrCreate(Labels.Item.getPropertyName(), iIdx);
            setItemProperty(itemNode,i);
            //Item mWord node-ok letrehoza
            HashSet<String> itemMWords = getUniqueItemMetaWordsByKey(iIdx, "VodMenuDirect", "[^\\p{L}0-9:_ ]");
            for (String mW : itemMWords) {
                metaWNode = mWordFactory.getOrCreate(Labels.Meta.getPropertyName(), mW);
                createUniqueRelationship(Relationships.HAS_META.getPropertyName(), iIdx + "_" + mW, Relationships.HAS_META, itemNode, metaWNode);
            }
            j++;
            if (j % 100 == 0) {
                tx.success();
                tx.close();
                tx = graphDB.beginTx();
                System.out.println(j);
            }
        }
        tx.success();
        tx.close();

        System.out.println("Az itemek feltoltese sikerult!");
    }

    public UniqueFactory<Node> createUniqueNodeFactory(final Labels nodeType, GraphDatabaseService graphDb) {
         return new UniqueFactory.UniqueNodeFactory(graphDb, nodeType.name()) {
            @Override
            protected void initialize(Node createdNode, Map<String, Object> properties) {
                createdNode.addLabel(nodeType);
                createdNode.setProperty(nodeType.getPropertyName(),properties.get(nodeType.getPropertyName()));
            }
        };
    }

    public Relationship createUniqueRelationship(String indexableKey, final String indexableValue,
                                                 final RelationshipType type, final Node start, final Node end) {

        UniqueFactory<Relationship> factory = new UniqueFactory.UniqueRelationshipFactory(graphDB, indexableKey) {
            @Override
            protected Relationship create(Map<String, Object> properties) {
                return start.createRelationshipTo(end, type);
            }
        };
        return factory.getOrCreate(indexableKey, indexableValue);
    }
    public HashSet<String> getUniqueItemMetaWordsByKey(int iIdx, String key, String key_value_separator){
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

    public void setItemProperty(Node node, Database.Item i){
        node.setProperty(Labels.Item.getPropertyName(),i.name);
    }

    public void listGraphDBInfo(){
        System.out.println("--------------------------------------");
        System.out.println("Adatbazis informaciok:");
        Transaction tx = graphDB.beginTx();
        Schema schema = graphDB.schema();
        System.out.println("---Label schema indexek:");
        for (IndexDefinition definition : schema.getIndexes())
            System.out.println(definition.toString());
        System.out.println("---Legacy indexek:");
        System.out.println("------Node indexek:");
        for (String s : graphDB.index().nodeIndexNames())
            System.out.println(s);
        System.out.println("------Relationship indexek:");
        for (String s : graphDB.index().relationshipIndexNames())
            System.out.println(s);
        Result r = graphDB.execute("MATCH (n:Item) RETURN COUNT(n)");
        System.out.print("Itemek szama: ");
        while(r.hasNext())
            System.out.println(r.next().values().toString());
        r = graphDB.execute("MATCH (n:User) RETURN COUNT(n)");
        System.out.print("Userek szama: ");
        while(r.hasNext())
            System.out.println(r.next().values().toString());
        r = graphDB.execute("MATCH (n:Meta) RETURN COUNT(n)");
        System.out.print("Metawordok szama: ");
        while(r.hasNext())
            System.out.println(r.next().values().toString());
        r = graphDB.execute("MATCH (n:Item)-[r]->(w:Meta) RETURN TYPE(r), COUNT(*)");
        System.out.println("Item-Metaword kapcsolatok szama: ");
        while(r.hasNext())
            System.out.println(r.next().toString());
        r = graphDB.execute("MATCH (n:User)-[r]->(w:Item) RETURN TYPE(r), COUNT(*)");
        System.out.println("Item-Metaword kapcsolatok szama: ");
        while(r.hasNext())
            System.out.println(r.next().toString());
        r = graphDB.execute("MATCH (n:Item)-[r]->(w:Meta) RETURN n.itemID, n.name, COUNT(*) AS CNT ORDER BY CNT DESC LIMIT 5");
        System.out.print("Item-Metaword legtobb kapcsolatu 5 Item: ");
        while(r.hasNext())
            System.out.println(r.next().toString());
        tx.success();
        tx.close();
    }
}
