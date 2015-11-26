package hf;


import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.traversal.TraversalDescription;

import java.util.HashSet;

public class WordCoSimGraphPredictor extends GraphDBPredictor {


    @Override
    public void trainFromGraphDB() {

    }

    public void computeSims(boolean uploadResultIntoDB) {

    }

    public int computeCosineSimilarity(Node nodeA, TraversalDescription description,
                                       Relationships existingRelType, HashSet<SimLink> similarities) {

        int computedSims = 0;

        return computedSims;
    }


    public void test() {

    }


    /**
     * Prediktalasra. Arra felkeszitve, hogy a usereken megy sorba a kiertekeles, nem itemeken!
     *
     * @param uIdx
     * @param iIdx
     * @return
     */
    public double predict(int uIdx, int iIdx, long time) {
        return 0.0;
    }

}
