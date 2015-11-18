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


public class CFGraphPredictor extends GraphDBPredictor {

    public void train(){
        super.train();
    }

    public void computeItemToItemSims(boolean uploadResultIntoDB) {
        LogHelper.INSTANCE.log("Start CFSim!");

        HashSet<SimLink> sims = new HashSet<>();
        ArrayList<Node> nodeList = graphDB.getNodesByLabel(Labels.Item);

        Transaction tx = graphDB.graphDBService.beginTx();

        TraversalDescription simNodeFinder = graphDB.graphDBService.traversalDescription()
                .depthFirst()
                .relationships(Relationships.SEEN)
                .evaluator(Evaluators.atDepth(2))
                .uniqueness(Uniqueness.RELATIONSHIP_PATH)
                .uniqueness(Uniqueness.NODE_PATH);

        long numOfComputedSims = 0;
        int changeCounter1 = 0;     //folyamat kiiratásához

//        1 node teszteléséhez:
//        ArrayList<Node> nodeList = new ArrayList<>();
//        nodeList.add(graphDB.graphDBService.getNodeById(79));

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

        if (uploadResultIntoDB) {
            LogHelper.INSTANCE.log("Upload computed similarities to DB:");
            graphDB.batchInsertSimilarities(sims, Similarities.CF_ISIM);
            LogHelper.INSTANCE.log("Upload computed similarities to DB Done!");
        }

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
            if (friends.value() > 1.0) {      //csak azokat a hasonlóságokat számolom ki, ahol a suppAB > 1
                computedSims++;     //hány hasonlóságot számoltunk ki
                int suppB = friendNodes.get(friends.key());
                double sim = friends.value() / (Math.sqrt(suppA) * Math.sqrt(suppB));
                similarities.add(new SimLink(startNodeID, friends.key(), sim));
//            System.out.println("A: " + suppA + " B: " + suppB + " AB: " + suppAB + " sim: " + sim);
            }
        }
        return computedSims;
    }



    public void test(){

    }


    int lastUser = -1;
    HashSet<Long> itemsSeenByUser = new HashSet<>();
    HashMap<Long, Double> simsForItem = new HashMap<>();

    /**
     * Prediktalasra. Arra felkeszitve, hogy a usereken megy sorba a kiertekeles, nem itemeken!
     * @param uIdx userID
     * @param iIdx itemID
     * @return
     */
    public double predict(int uIdx, int iIdx, int method){
        /*
        Kétféle megközelítés:
        A vizsgált item és a user által látott egyes itemek között levő sim értékek szummáját:
        1) a User által látott összes film számával átlagolom --> prediction / user.getDegree(Relationships.SEEN)
        2) a User által látott filmek közül az adott itemmel sim kapcsolatban állók számával átlagolom
            --> prediction / matches
         */
        int matches =  0;
        double prediction = 0;
        Node user = graphDB.graphDBService.findNode(Labels.User, Labels.User.getIDName(), uIdx);
        if( uIdx != lastUser) {
            itemsSeenByUser = graphDB.getAllNeighborIDsByRel(user, Relationships.SEEN);
        }

        simsForItem = graphDB.getAllNeighborIDsBySim(graphDB.graphDBService.findNode(Labels.Item, Labels.Item.getIDName(),
                iIdx),Similarities.CF_ISIM);
        if(simsForItem.size() < itemsSeenByUser.size()) {
            for (Map.Entry<Long, Double> entry : simsForItem.entrySet()) {
                if (itemsSeenByUser.contains(entry.getKey())) {
                    prediction += entry.getValue();
                    matches++;
                }

            }
        }
        else {
            for (Long l : itemsSeenByUser) {
                if (simsForItem.containsKey(l)) {
                    prediction += simsForItem.get(l);
                    matches++;
                }

            }
        }

        if(method == 1)
            prediction = prediction / user.getDegree(Relationships.SEEN);  //1-es módszer
        else
            prediction = matches > 0 ? prediction / matches : 0.0;         //2-es módszer

        return prediction;
    }
}
