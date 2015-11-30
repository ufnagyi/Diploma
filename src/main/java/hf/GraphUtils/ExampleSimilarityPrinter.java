package hf.GraphUtils;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

public class ExampleSimilarityPrinter {
    private GraphDB graphDB;
    private String outputName;
    private Calendar actualTime;
    private HashMap<Node, Map<Node, Double>> exampleNodes;

    public ExampleSimilarityPrinter(GraphDB graphDB){
        this.graphDB = graphDB;
        this.exampleNodes = new HashMap<>();
    }

    /**
     * @param topNByNode Az elso N elem megkeresese
     * @param similarity Milyen hasonlosag szerint
     * @param label      Mely node-ok kozott?
     */
    public void printExampleSimilarityResults(int topNByNode, Similarities similarity, Labels label) {
        LogHelper.INSTANCE.log("i2i Recommender List nyotmatása fájlba:");

        if(!graphDB.isInited())
            graphDB.initDB();
        Transaction tx = graphDB.startTransaction();


        long minNodeIDByLabel = graphDB.getMinNodeIDByLabel(label);

        populateExamples(minNodeIDByLabel);
        for (Map.Entry<Node, Map<Node, Double>> cursor : exampleNodes.entrySet()) {
            cursor.setValue(graphDB.getTopNNeighborsAndSims(cursor.getKey(), similarity, topNByNode));
        }
        outputName = "data/impresstv_vod_dataset/" + "i2i_" + similarity.name() + "_" + actualTime.getTimeInMillis() + ".txt";
        try {
            BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(outputName));

            for (Map.Entry<Node, Map<Node, Double>> cursor : exampleNodes.entrySet()) {
                Map<Node, Double> topN = cursor.getValue();
                bufferedWriter.write("TopN list for " + cursor.getKey().getProperty(label.getPropertyName()) + ": ");
                bufferedWriter.newLine();
                printTopN(topN, label, bufferedWriter);
            }
            bufferedWriter.close();
        } catch (IOException e) {
            System.err.println("Caught IOException:" + e.getMessage());
            return;
        }
        graphDB.endTransaction(tx);
        LogHelper.INSTANCE.log("i2i Recommender List nyotmatása fájlba KÉSZ!");
    }

    private void populateExamples(long minNodeIDByLabel) {
        actualTime = Calendar.getInstance();
        int step = actualTime.get(Calendar.SECOND);
        for (int j = 0; j < 10; j++) {
            exampleNodes.put(graphDB.graphDBService.getNodeById(minNodeIDByLabel + j * step),null);
        }
    }

    public HashMap<Node, Map<Node, Double>> getExampleNodes(){
        return exampleNodes;
    }

    private void printTopN(Map<Node, Double> topN, Labels l, BufferedWriter bufferedWriter) throws IOException {
        int i = 1;
        for (Map.Entry<Node, Double> cursor : topN.entrySet()) {
            bufferedWriter.write(i + ". " + cursor.getKey().getProperty(l.getPropertyName()) + "  " + cursor.getValue());
            bufferedWriter.newLine();
            i++;
        }
        bufferedWriter.newLine();
    }
}
