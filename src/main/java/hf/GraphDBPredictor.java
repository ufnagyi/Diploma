package hf;

import onlab.core.Database;
import org.neo4j.graphdb.GraphDatabaseService;

/**
 * Created by ufnagyi
 */
public class GraphDBPredictor {
    public GraphDB graphDB;
    public Database db;

    public GraphDBPredictor(){
        graphDB = new GraphDB();
    }

    public void setParameters(Database db){
        this.db = db;
    }

    public void train(){
        graphDB.buildDB(db);
        System.out.println("A grafDB felepult!");
    }
}
