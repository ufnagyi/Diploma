package hf;

import onlab.core.Database;
import onlab.core.evaluation.Evaluation;
import onlab.core.predictor.Predictor;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by ufnagyi
 */
abstract public class GraphDBPredictor extends Predictor {
    public GraphDB graphDB;
    public Similarities sim;

    public void setParameters(GraphDB gDB, Database db, Similarities sim){
        graphDB = gDB;
        this.db = db;
        this.sim = sim;
    }

    public void train(boolean uploadResultIntoDB) {
        if (!this.graphDB.isInited())
            graphDB.initDB();
        this.computeSims(uploadResultIntoDB);
    }

    public abstract void computeSims(boolean uploadResultIntoDB);

    public abstract void trainFromGraphDB();

    public void train(Database db, Evaluation eval){}

}
