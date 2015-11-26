package hf;

import onlab.core.Database;
import onlab.core.evaluation.Evaluation;
import org.neo4j.graphdb.Node;

import java.util.HashSet;

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
