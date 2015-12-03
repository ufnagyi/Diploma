package hf.GraphPredictors;

import gnu.trove.map.hash.TLongIntHashMap;
import hf.GraphUtils.*;
import onlab.core.Database;
import onlab.core.evaluation.Evaluation;
import onlab.core.predictor.Predictor;
import org.neo4j.graphdb.Node;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;

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

    public static int getNodeDegreeFromMap(Node node, long nodeID, TLongIntHashMap nodeDegrees, Relationships rel){
        int suppNode;
        if(nodeDegrees.containsKey(nodeID))
            suppNode = nodeDegrees.get(nodeID);
        else {
            suppNode = GraphDB.getDistinctDegree(node, rel);
            nodeDegrees.put(nodeID,suppNode);
        }
        return suppNode;
    }

    public void printComputedSimilarityResults(HashSet<SimLink<Long>> simLinks, boolean uploadResultIntoDB){
        System.out.println("Num of computed sims: " + simLinks.size());
        LogHelper.INSTANCE.log("Stop " + sim.name() + "!");

        if (uploadResultIntoDB) {
            LogHelper.INSTANCE.log("Upload computed similarities to DB:");
            graphDB.batchInsertSimilarities(simLinks, sim);
            LogHelper.INSTANCE.log("Upload computed similarities to DB Done!");
        }

        ArrayList<Double> vals = new ArrayList<>(simLinks.size());
        double sum = 0.0;
        for (SimLink s : simLinks) {
            sum += s.similarity;
            vals.add(s.similarity);
        }
        double avg = sum / simLinks.size();
        Collections.sort(vals);
        System.out.println("Max sim: " + vals.get(vals.size()-1));
        System.out.println("Min sim: " + vals.get(0));
        System.out.println("Avg sim: " + avg);
        System.out.println("Median sim: " + vals.get((vals.size()/2)));
        System.out.println("1st decile sim: " + vals.get((vals.size()/10)));
        System.out.println("2st decile sim: " + vals.get((vals.size()/10*2)));
    }

}
