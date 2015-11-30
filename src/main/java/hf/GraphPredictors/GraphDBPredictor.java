package hf.GraphPredictors;

import hf.GraphUtils.GraphDB;
import hf.GraphUtils.Similarities;
import onlab.core.Database;
import onlab.core.evaluation.Evaluation;
import onlab.core.predictor.Predictor;

/**
 * Created by ufnagyi
 */
abstract public class GraphDBPredictor extends Predictor {
    public GraphDB graphDB;
    public Similarities sim;

    public void setParameters(GraphDB gDB, Database db){
        graphDB = gDB;
        this.db = db;
    }

    public void train(boolean uploadResultIntoDB) {
        if (!this.graphDB.isInited())
            graphDB.initDB();
        this.computeSims(uploadResultIntoDB);
    }

    protected abstract void computeSims(boolean uploadResultIntoDB);

    public abstract void trainFromGraphDB();

    public void train(Database db, Evaluation eval){}

}
