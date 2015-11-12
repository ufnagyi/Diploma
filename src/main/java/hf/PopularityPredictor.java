package hf;

import org.neo4j.graphdb.Node;

import java.util.HashSet;

public class PopularityPredictor extends GraphDBPredictor {


    public void computePopularity(Node startNode, Relationships relationship) {
        HashSet<Long> allNeighborsByRel = graphDB.getAllNeighborsByRel(startNode, relationship);
    }

    public void test() {

    }
}
