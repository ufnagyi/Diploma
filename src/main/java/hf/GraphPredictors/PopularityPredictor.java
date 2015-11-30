package hf.GraphPredictors;

import hf.GraphUtils.Relationships;
import org.neo4j.graphdb.Node;

public class PopularityPredictor extends GraphDBPredictor {


    public void computePopularity(Node startNode, Relationships relationship) {

    }

    @Override
    public void trainFromGraphDB() {

    }

    @Override
    public void computeSims(boolean uploadResultIntoDB) {

    }

    public double predict(int uIdx, int iIdx, long time){
        return 0.0;
    }
}
