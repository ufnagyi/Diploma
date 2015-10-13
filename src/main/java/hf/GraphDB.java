package hf;

import onlab.core.Database;
import onlab.core.ExtendedDatabase;
import org.neo4j.cypher.internal.compiler.v1_9.commands.Has;
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
    public ExtendedDatabase dbExt = null;

    public enum Labels implements Label {
        ITEM, USER, META;
    }

    public enum Relationships implements RelationshipType {
        SEEN, HAS_META;
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
        dbExt = (ExtendedDatabase) db;
        graphDB = new GraphDatabaseFactory().newEmbeddedDatabase("C:/Users/ufnagyi/Documents/Neo4J_Database");
        registerShutdownHook(graphDB);

        loadItemsToGraphDB(db);
        //loadUsersToGraphDB(db);
        createIndexes();

    }

    private void createIndexes() {
        Transaction tx = graphDB.beginTx();
        Schema schema = graphDB.schema();

        //item és user indexelés ID szerint
        IndexDefinition indexDefinition;
        indexDefinition = schema.indexFor(Labels.ITEM).on("ItemID").create();
        indexDefinition = schema.indexFor(Labels.USER).on("UserID").create();
        for (IndexDefinition definition : schema.getIndexes())
            definition.toString();
        tx.success();
        tx.close();
    }


    //TODO
    private void loadRelationshipsToGraphDB(Database db) {
        int j = 0;
        Node node;
        Transaction tx = graphDB.beginTx();
        UniqueFactory<Node> factory = new UniqueFactory.UniqueNodeFactory(graphDB, "USER") {
            @Override
            protected void initialize(Node created, Map<String, Object> properties) {
                created.setProperty("UserID", properties.get("UserID"));
            }
        };

        //Userek feltöltése a gráfba
        for (Database.User u : db.users(null)) {
            node = factory.getOrCreate("UserID", u.idx);
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

    private void loadUsersToGraphDB(Database db) {
        int j = 0;
        Node node;
        Transaction tx = graphDB.beginTx();
        UniqueFactory<Node> factory = new UniqueFactory.UniqueNodeFactory(graphDB, "USER") {
            @Override
            protected void initialize(Node created, Map<String, Object> properties) {
                created.setProperty("UserID", properties.get("UserID"));
            }
        };

        //Userek feltöltése a gráfba
        for (Database.User u : db.users(null)) {
            node = factory.getOrCreate("UserID", u.idx);
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

    private void loadItemsToGraphDB(Database db) {
        int j = 0;
        Node node;
        HashSet<String> allUniqueMetaWords = new HashSet<String>(100);
        Transaction tx = graphDB.beginTx();
//        Schema schema = graphDB.schema();
//        for (IndexDefinition definition : schema.getIndexes())
//            definition.toString();

        UniqueFactory<Node> factory = new UniqueFactory.UniqueNodeFactory(graphDB, "ITEM") {
            @Override
            protected void initialize(Node created, Map<String, Object> properties) {
                created.setProperty("ItemID", properties.get("ItemID"));
            }
        };

        String cypherCommand;
        //Itemek feltöltése a gráfba
        for (Database.Item i : db.items(null)) {
            int iIdx = i.idx;
            node = factory.getOrCreate("ItemID", iIdx);
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

    public HashSet<String> getUniqueItemMetaWordsByKey(int iIdx, String key, String key_value_separator, HashSet<String> allUniqueMetaWords_ ){
        HashSet<String> itemMetaWords = new HashSet<String>();
        String keyAll = dbExt.getItemKeyValue(iIdx, key);
        String[] values = keyAll.split(key_value_separator);

        for (String val : values) {
            if (val.equals("") || val.length() < 3 || allUniqueMetaWords_.contains(val))
                continue;
            String mWord = val.toLowerCase();
            itemMetaWords.add(mWord);
            allUniqueMetaWords_.add(mWord);
        }
        return itemMetaWords;
    }

    public void setItemProperty(Node node, Database.Item i){
        node.addLabel(Labels.ITEM);
        node.setProperty("ItemID",i.idx);
        node.setProperty("Name",i.name);
    }

    public void setUserProperty(Node node, Database.User u){
        node.addLabel(Labels.USER);
        node.setProperty("UserID",u.idx);
    }
}
