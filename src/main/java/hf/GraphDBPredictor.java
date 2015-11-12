package hf;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by ufnagyi
 */
abstract public class GraphDBPredictor {
    public GraphDB graphDB;

    public void setParameters(GraphDB gDB){
        graphDB = gDB;
    }

    public void train() {
        if (!this.graphDB.isInited())
            graphDB.initDB();
    }

    /**
     * @param topNByNode Az elso N elem megkeresese
     * @param similarity Milyen hasonlosag szerint
     * @param label      Mely node-ok kozott?
     */
    public void exampleSimilarityResults(int topNByNode, Similarities similarity, Labels label) {
        Transaction tx = graphDB.graphDBService.beginTx();
        HashMap<Node, Map<Node, Double>> exampleNodes = new HashMap<>();

        Calendar calendar = Calendar.getInstance();
        int step = calendar.get(Calendar.SECOND);
        for (int j = 0; j < 10; j++) {
            exampleNodes.put(graphDB.graphDBService.findNode(label, label.getIDName(), Integer.toString(79 + j * step)), null);
        }
        for (Map.Entry<Node, Map<Node, Double>> cursor : exampleNodes.entrySet()) {
            cursor.setValue(graphDB.getTopNNeighborsAndSims(cursor.getKey(), similarity, topNByNode));
        }
        for (Map.Entry<Node, Map<Node, Double>> cursor : exampleNodes.entrySet()) {
            Map<Node, Double> topN = cursor.getValue();
            System.out.println("TopN list for " + cursor.getKey().getProperty(label.getPropertyName()) + ": ");
            printTopN(topN, label);
        }
        tx.success();
        tx.close();
    }

    private void printTopN(Map<Node, Double> topN, Labels l) {
        int i = 1;
        for (Map.Entry<Node, Double> cursor : topN.entrySet()) {
            System.out.println(i + ". " + cursor.getKey().getProperty(l.getPropertyName()) + "  " + cursor.getValue());
            i++;
        }
    }


    //predictor teszteléséhez
    public abstract void test();
}
