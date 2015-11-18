package hf;

import org.neo4j.graphdb.Node;

import java.util.HashSet;

public class PopularityPredictor extends GraphDBPredictor {


    public void computePopularity(Node startNode, Relationships relationship) {
        HashSet<Long> allNeighborsByRel = graphDB.getAllNeighborIDsByRel(startNode, relationship);
    }

    public void test() {

    }

    public double predict(int uIdx, int iIdx){
        return 0.0;
    }
}
