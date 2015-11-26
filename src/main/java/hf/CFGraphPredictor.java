package hf;

import gnu.trove.iterator.TObjectIntIterator;
import gnu.trove.map.hash.TObjectIntHashMap;
import onlab.core.Database;
import onlab.core.evaluation.Evaluation;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Uniqueness;

import java.util.*;


public class CFGraphPredictor extends GraphDBPredictor {

    private HashSet<SimLink> sims;
    private int method;

    /**
     *
     * @param graphDB A grafDB, amire epitkezik
     * @param method Prediction szamitasi modszer 1-es: /összes, 2-es: /match
     */
    public void setParameters(GraphDB graphDB, int method){
        this.graphDB = graphDB;
        this.method = method;
    }

    public void trainFromGraphDB(){

        Transaction tx = graphDB.startTransaction();
        sims = graphDB.getAllSimilarities(Labels.Item, Similarities.CF_ISIM);
    }

    public void computeSims(boolean uploadResultIntoDB) {
        LogHelper.INSTANCE.log("Start CFSim!");

        sims = new HashSet<>();
        ArrayList<Node> nodeList = graphDB.getNodesByLabel(Labels.Item);

        Transaction tx = graphDB.startTransaction();

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
//        nodeList.add(graphDB.graphDBService.getNodeById(8));

        for (Node item : nodeList) {
            int changes = computeCosineSimilarity(item, simNodeFinder, Relationships.SEEN, sims);
            numOfComputedSims += changes;
            changeCounter1 += changes;
            if(changeCounter1 > 50000){
                graphDB.endTransaction(tx);
                tx = graphDB.startTransaction();
                changeCounter1 = 0;
                System.out.println(numOfComputedSims);
            }
        }
        graphDB.endTransaction(tx);
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
            if (!similarities.contains(new SimLink(startNodeID, friendNodeId,0.0))) {        //ha még nem lett kiszámolva a hasonlóságuk
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


    private int lastUser = -1;
    private Node user = null;
    private HashSet<Long> itemsSeenByUser = new HashSet<>();
    private HashMap<Long, Double> simsForItem = new HashMap<>();
    private int numUser = 0;

    /**
     * Prediktalasra. Arra felkeszitve, hogy a usereken megy sorba a kiertekeles, nem itemeken!
     * @param uID userID
     * @param iID itemID
     * @return
     */
    public double predict(int uID, int iID, long time){
        /*
        Kétféle megközelítés:
        A vizsgált item és a user által látott egyes itemek között levő sim értékek szummáját:
        1) a User által látott összes film számával átlagolom --> prediction / user.getDegree(Relationships.SEEN)
        2) a User által látott filmek közül az adott itemmel sim kapcsolatban állók számával átlagolom
            --> prediction / matches
         */
        int matches =  0;
        double prediction = 0.0;
        if( uID != lastUser) {
            user = graphDB.graphDBService.findNode(Labels.User, Labels.User.getIDName(), uID);
            itemsSeenByUser = graphDB.getAllNeighborIDsByRel(user, Relationships.SEEN);
            lastUser = uID;
            numUser++;
            if(numUser % 10 == 0)
                System.out.println(numUser);
        }

        simsForItem = graphDB.getAllNeighborIDsBySim(graphDB.graphDBService.findNode(Labels.Item, Labels.Item.getIDName(),
                iID),Similarities.CF_ISIM);
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

        if(method == 1) {
            int userRelDegree = user.getDegree(Relationships.SEEN);
            prediction = userRelDegree > 0 ? (prediction / userRelDegree) : 0.0;  //1-es módszer
        }
        else
            prediction = matches > 0 ? prediction / matches : 0.0;         //2-es módszer

        return prediction;
    }
}
