package hf;

import onlab.core.Database;
import onlab.core.evaluation.Evaluation;
import onlab.core.predictor.Predictor;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by ufnagyi
 */
abstract public class GraphDBPredictor extends Predictor {
    public GraphDB graphDB;

    public void setParameters(GraphDB gDB){
        graphDB = gDB;
    }

    public void train(boolean uploadResultIntoDB) {
        if (!this.graphDB.isInited())
            graphDB.initDB();
        this.computeSims(uploadResultIntoDB);
    }

    public abstract void computeSims(boolean uploadResultIntoDB);

    public abstract void trainFromGraphDB();

    public void train(Database db, Evaluation eval){}

    /**
     * @param topNByNode Az elso N elem megkeresese
     * @param similarity Milyen hasonlosag szerint
     * @param label      Mely node-ok kozott?
     */
    public void exampleSimilarityResults(int topNByNode, Similarities similarity, Labels label) {
        if(!graphDB.isInited())
            graphDB.initDB();
        Transaction tx = graphDB.startTransaction();
        HashMap<Node, Map<Node, Double>> exampleNodes = new HashMap<>();

        long minNodeIDByLabel = graphDB.getMinNodeIDByLabel(label);

        Calendar calendar = Calendar.getInstance();
        int step = calendar.get(Calendar.SECOND);
        for (int j = 0; j < 10; j++) {
            exampleNodes.put(graphDB.graphDBService.getNodeById(minNodeIDByLabel + j * step),null);
        }
        for (Map.Entry<Node, Map<Node, Double>> cursor : exampleNodes.entrySet()) {
            cursor.setValue(graphDB.getTopNNeighborsAndSims(cursor.getKey(), similarity, topNByNode));
        }
        for (Map.Entry<Node, Map<Node, Double>> cursor : exampleNodes.entrySet()) {
            Map<Node, Double> topN = cursor.getValue();
            System.out.println("TopN list for " + cursor.getKey().getProperty(label.getPropertyName()) + ": ");
            printTopN(topN, label);
        }
        graphDB.endTransaction(tx);
    }

    private void printTopN(Map<Node, Double> topN, Labels l) {
        int i = 1;
        for (Map.Entry<Node, Double> cursor : topN.entrySet()) {
            System.out.println(i + ". " + cursor.getKey().getProperty(l.getPropertyName()) + "  " + cursor.getValue());
            i++;
        }
    }

}
