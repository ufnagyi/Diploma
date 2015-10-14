package hf;

import onlab.core.Database;
import onlab.core.ExtendedDatabase;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
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
        Item, User, Meta
    }

    public enum indexedProperties {
        Item("itemID"), User("userID"), Meta("mword");
        private String property;

        private indexedProperties(String str) {
            this.property = str;
        }
    }

    public enum Relationships implements RelationshipType {
        SEEN, HAS_META
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

    public void buildDB(Database db) {
        this.db = db;
        this.dbExt = (ExtendedDatabase) db;

        graphDB = new GraphDatabaseFactory().newEmbeddedDatabase("C:/Users/ufnagyi/Documents/Neo4J_Database");
        registerShutdownHook(graphDB);

        loadMetaWordsToGraphDB();
        loadItemsToGraphDB();
        loadUsersToGraphDB();
        createIndexes();

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
        if(!checkForIndex(schema.getIndexes(Labels.Item),indexedProperties.Item.property)) {
            IndexDefinition indexDefinition = schema.indexFor(Labels.Item).on(indexedProperties.Item.property).create();
            tx.success();
            tx.close();
            tx = graphDB.beginTx();
        }
        if(!checkForIndex(schema.getIndexes(Labels.User),indexedProperties.User.property)) {
            IndexDefinition indexDefinition = schema.indexFor(Labels.User).on(indexedProperties.User.property).create();
            tx.success();
            tx.close();
            tx = graphDB.beginTx();
        }
        if(!checkForIndex(schema.getIndexes(Labels.Meta),indexedProperties.Meta.property)) {
            IndexDefinition indexDefinition = schema.indexFor(Labels.Meta).on(indexedProperties.Meta.property).create();
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

        Node node;
        Transaction tx = graphDB.beginTx();
        UniqueFactory<Node> factory = new UniqueFactory.UniqueNodeFactory(graphDB, "Meta") {
            @Override
            protected void initialize(Node created, Map<String, Object> properties) {
                created.setProperty("mword", properties.get("mword"));
            }
        };

        //Metawordok feltöltése a gráfba
        Iterator<String> iterator = allUniqueMetaWords.iterator();
        int j = 0;
        while(iterator.hasNext()) {
            String mW = iterator.next();
            node = factory.getOrCreate("mword",mW);
            setMetaWordProperty(node,mW);
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

    //TODO
    private void loadRelationshipsToGraphDB() {
    }

    private void loadUsersToGraphDB() {
        int j = 0;
        Node node;
        Transaction tx = graphDB.beginTx();
        UniqueFactory<Node> factory = new UniqueFactory.UniqueNodeFactory(graphDB, "User") {
            @Override
            protected void initialize(Node created, Map<String, Object> properties) {
                created.setProperty("userID", properties.get("userID"));
            }
        };

        //Userek feltöltése a gráfba
        for (Database.User u : db.users(null)) {
            node = factory.getOrCreate("userID", u.idx);
            setUserProperty(node, u);
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

    private void loadItemsToGraphDB() {
        int j = 0;
        Node node;
        Transaction tx = graphDB.beginTx();
//        Schema schema = graphDB.schema();
//        for (IndexDefinition definition : schema.getIndexes())
//            definition.toString();

        UniqueFactory<Node> factory = new UniqueFactory.UniqueNodeFactory(graphDB, "Item") {
            @Override
            protected void initialize(Node created, Map<String, Object> properties) {
                created.setProperty("itemID", properties.get("itemID"));
            }
        };

        //Itemek feltöltése a gráfba
        for (Database.Item i : db.items(null)) {
            int iIdx = i.idx;
            node = factory.getOrCreate("itemID", iIdx);
            setItemProperty(node, i);
            j++;
            if (j % 500 == 0) {
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
        node.addLabel(Labels.Item);
        node.setProperty("itemID",i.idx);
        node.setProperty("name",i.name);
    }

    public void setUserProperty(Node node, Database.User u){
        node.addLabel(Labels.User);
        node.setProperty("userID",u.idx);
    }

    public void setMetaWordProperty(Node node, String metaWord){
        node.addLabel(Labels.Meta);
        node.setProperty("mword", metaWord);
    }
}
