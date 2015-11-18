package hf;


import gnu.trove.iterator.TObjectIntIterator;
import gnu.trove.map.hash.TObjectIntHashMap;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Uniqueness;

import java.util.*;

public class WordCoSimGraphPredictor extends GraphDBPredictor {


    public void train() {
        super.train();
    }

    public void computeWordToWordSims(boolean uploadResultIntoDB) {

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
    public double predict(int uIdx, int iIdx, int method) {
        return 0.0;
    }

}
