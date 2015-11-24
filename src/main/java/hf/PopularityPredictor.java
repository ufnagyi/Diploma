package hf;

import onlab.core.Database;
import onlab.core.evaluation.Evaluation;
import org.neo4j.graphdb.Node;

import java.util.HashSet;

public class PopularityPredictor extends GraphDBPredictor {


    public void computePopularity(Node startNode, Relationships relationship) {
        HashSet<Long> allNeighborsByRel = graphDB.getAllNeighborIDsByRel(startNode, relationship);
    }

    public void train(Database db, Evaluation e){}

    public void test() {

    }

    public double predict(int uIdx, int iIdx, long time){
        return 0.0;
    }
}
