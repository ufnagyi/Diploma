package hf;

import gnu.trove.iterator.TObjectIntIterator;
import gnu.trove.map.hash.TObjectIntHashMap;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.TraversalDescription;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;


public class CFGraphPredictor extends GraphDBPredictor {

    public void train(){
        super.train();
    }

    public void computeItemToItemSims() {
        LogHelper.INSTANCE.log("Start CFSim!");

//        System.out.println(graphDB.graphDBService.getNodeById((long) 16).getProperty(Labels.Item.getPropertyName()));
        Transaction tx = graphDB.graphDBService.beginTx();

        HashSet<SimLink> sims = new HashSet<>(5000000);
        ArrayList<Node> nodeList = graphDB.getNodesByLabel(Labels.Item);
        TraversalDescription simNodeFinder = graphDB.graphDBService.traversalDescription()
                .depthFirst()
                .relationships(Relationships.SEEN)
                .evaluator(Evaluators.atDepth(2));

        long numOfComputedSims = 0;
        int changeCounter1 = 0;     //folyamat kiiratásához
//        int changeCounter2 = 0;     //5 millió elemenként kiiratás

        for (Node item : nodeList) {
            int changes = computeCosineSimilarity(item, simNodeFinder, Relationships.SEEN, sims);
            numOfComputedSims += changes;
            changeCounter1 += changes;
            if(changeCounter1 > 50000){
                tx.success();
                tx.close();
                tx = graphDB.graphDBService.beginTx();
                changeCounter1 = 0;
                System.out.println(numOfComputedSims);
            }
        }
        tx.success();
        tx.close();
        System.out.println("Num of computed sims: " + numOfComputedSims);
        LogHelper.INSTANCE.log("Stop CFSim!");

        ArrayList<Double> vals = new ArrayList<>(sims.size());
        double sum = 0.0;
        for (SimLink s : sims) {
            sum += s.similarity;
            vals.add(s.similarity);
        }
        double avg = sum / sims.size();
        Collections.sort(vals);
        System.out.println("Max sim: " + vals.get(vals.size()-1));
        System.out.println("Min sim: " + vals.get(0));
        System.out.println("Avg sim: " + avg);
        System.out.println("Median sim: " + vals.get((vals.size()/2)));
        System.out.println("1st decile sim: " + vals.get((vals.size()/10)));
        System.out.println("2st decile sim: " + vals.get((vals.size()/10*2)));
    }

    public int computeCosineSimilarity(Node nodeA, TraversalDescription description,
                                       Relationships existingRelType, HashSet<SimLink> similarities) {

        int computedSims = 0;

        TObjectIntHashMap<Long> friendNodes = new TObjectIntHashMap<>();    //friendNode_B --> suppB
        TObjectIntHashMap<Long> suppABForAllB = new TObjectIntHashMap<>();  //friendNode_B --> suppAB

        long startNodeID = nodeA.getId();
        int suppA = nodeA.getDegree(existingRelType);

        for (Path path : description.traverse(nodeA)) {
            Node friendNode = path.endNode();
            long friendNodeId = friendNode.getId();
            if (!similarities.contains(new SimLink(startNodeID, friendNodeId))) {        //ha még nem lett kiszámolva a hasonlóságuk
                friendNodes.put(friendNodeId, friendNode.getDegree(existingRelType));      //suppB
                suppABForAllB.adjustOrPutValue(friendNodeId, 1, 1);                    //suppAB növelés
            }
        }

        TObjectIntIterator<Long> friends = suppABForAllB.iterator();
        while (friends.hasNext()) {
            friends.advance();
//            if (friends.value() < 2) {      //csak azokat a hasonlóságokat számolom ki, ahol a suppAB > 1
//                continue;
//            }
            computedSims++;     //hány hasonlóságot számoltunk ki
            int suppB = friendNodes.get(friends.key());
            double suppAB = (double) friends.value();

            double sim = suppAB / (Math.sqrt(suppA) * Math.sqrt(suppB));
            similarities.add(new SimLink(startNodeID, friends.key(), sim));
//            System.out.println("A: " + suppA + " B: " + suppB + " AB: " + suppAB + " sim: " + sim);
        }
        return computedSims;
    }

    public void test(){
    }
}
