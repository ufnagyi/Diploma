package hf.GraphPredictors;

import gnu.trove.map.hash.TLongIntHashMap;
import hf.GraphUtils.*;
import hf.Main;
import onlab.core.Database;
import onlab.core.evaluation.Evaluation;
import onlab.core.predictor.Predictor;
import org.neo4j.graphdb.Node;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;

abstract public class GraphDBPredictor extends Predictor {
    public GraphDB graphDB;
    public Similarities sim;

    public void setParameters(GraphDB gDB) {
        graphDB = gDB;
        this.db = gDB.db;
    }

    public abstract String getName();

    public abstract String getShortName();

    public void train(boolean uploadResultIntoDB) {
        graphDB.initDB();
        this.computeSims(uploadResultIntoDB);
    }

    protected abstract void computeSims(boolean uploadResultIntoDB);

    public abstract void printParameters();

    public abstract void trainFromGraphDB();

    public void train(Database db, Evaluation eval) {
    }

    public static int getNodeDegreeFromMap(Node node, long nodeID, TLongIntHashMap nodeDegrees, Relationships rel) {
        int suppNode;
        if (nodeDegrees.containsKey(nodeID))
            suppNode = nodeDegrees.get(nodeID);
        else {
            suppNode = GraphDB.getDistinctDegree(node, rel);
            nodeDegrees.put(nodeID, suppNode);
        }
        return suppNode;
    }

    public void printComputedSimilarityResults(HashSet<SimLink<Long>> simLinks, boolean uploadResultIntoDB) {
        LogHelper.INSTANCE.logToFileT("Num of computed sims: " + simLinks.size());
        LogHelper.INSTANCE.logToFileT("Stop " + sim.name() + "!");

        if (uploadResultIntoDB) {
            LogHelper.INSTANCE.logToFileT("Upload computed similarities to DB:");
            graphDB.batchInsertSimilarities(simLinks, sim);
            LogHelper.INSTANCE.logToFileT("Upload computed similarities to DB Done!");
        }

        ArrayList<Double> vals = new ArrayList<>(simLinks.size());
        double sum = 0.0;
        for (SimLink s : simLinks) {
            sum += s.similarity;
            vals.add(s.similarity);
        }
        double avg = sum / simLinks.size();
        Collections.sort(vals);
        LogHelper.INSTANCE.logToFile("Max sim: " + vals.get(vals.size() - 1));
        LogHelper.INSTANCE.logToFile("Min sim: " + vals.get(0));
        LogHelper.INSTANCE.logToFile("Avg sim: " + avg);
        LogHelper.INSTANCE.logToFile("Median sim: " + vals.get((vals.size() / 2)));
        LogHelper.INSTANCE.logToFile("1st decile sim: " + vals.get((vals.size() / 10)));
        LogHelper.INSTANCE.logToFile("2st decile sim: " + vals.get((vals.size() / 10 * 2)));
        LogHelper.INSTANCE.logToFile("Database size: " + graphDB.getDBSize() + " MB");
        LogHelper.INSTANCE.logToFile("Uploaded sims size: " + (graphDB.getDBSize() - graphDB.initialDBSize) + " MB");
        LogHelper.INSTANCE.printMemUsage();
    }
}
