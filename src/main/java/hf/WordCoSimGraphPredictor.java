package hf;


import com.google.common.collect.Ordering;
import gnu.trove.iterator.TObjectIntIterator;
import gnu.trove.map.hash.TLongDoubleHashMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import gnu.trove.map.hash.TObjectLongHashMap;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.traversal.*;

import java.util.*;

public class WordCoSimGraphPredictor extends GraphDBPredictor {

    private TObjectLongHashMap VOD_IDs;

    @Override
    public void trainFromGraphDB() {

    }

    public void buildUserProfiles(){
        LogHelper.INSTANCE.log("CBSim with UserProfile");
        Transaction tx = graphDB.startTransaction();
        ArrayList<Node> itemList = graphDB.getNodesByLabel(Labels.Item);
        double numOfItems = (double) itemList.size();
        ArrayList<Node> userList = graphDB.getNodesByLabel(Labels.User);
        ArrayList<Node> VODList = graphDB.getNodesByLabel(Labels.VOD);

        VOD_IDs = new TObjectLongHashMap();

        // word_IDF = numOfItems / numOfItemsContainingTheWord
        TLongDoubleHashMap wordIDF = new TLongDoubleHashMap();
        for(Node n : VODList){
            wordIDF.put(n.getId(), Math.log(numOfItems / n.getDegree(Relationships.HAS_META)));
            VOD_IDs.put(n.getProperty(Labels.VOD.getPropertyName()),n.getId());
        }
        TraversalDescription VODsOfItemsSeenByUser = graphDB.graphDBService.traversalDescription()
                .depthFirst()
                .expand( new PathExpander<Object>() {
                    @Override
                    public Iterable<Relationship> expand(Path path, BranchState<Object> objectBranchState) {
                        int depth = path.length();
                        if( depth == 0 ) {
                            return path.endNode().getRelationships(
                                    Relationships.SEEN );
                        }
                        else {
                            return path.endNode().getRelationships(
                                    Relationships.HAS_META );
                        }
                    }
                    @Override
                    public PathExpander<Object> reverse() {
                        return null;
                    }
                })
                .evaluator( Evaluators.atDepth( 2 ) )
                .evaluator(path -> {
                    if( path.endNode().hasLabel( Labels.VOD ) ) {
                        return Evaluation.INCLUDE_AND_CONTINUE;
                    }
                    return Evaluation.EXCLUDE_AND_CONTINUE;
                });

        graphDB.endTransaction(tx);
        LogHelper.INSTANCE.log("CBSim with UserProfile KESZ");
    }


    /**
     * CosineSim alapu CBF
     * @param uploadResultIntoDB
     */
    public void computeSims(boolean uploadResultIntoDB) {
        LogHelper.INSTANCE.log("Start CBFSim!");

        Transaction tx = graphDB.startTransaction();
        ArrayList<Node> itemList = graphDB.getNodesByLabel(Labels.Item);

        HashSet<SimLink<Long>> simLinks = new HashSet<>();

        TraversalDescription simNodeFinder = graphDB.graphDBService.traversalDescription()
                .depthFirst()
                .relationships(Relationships.HAS_META)
                .evaluator(Evaluators.atDepth(2))
                .uniqueness(Uniqueness.RELATIONSHIP_PATH)
                .uniqueness(Uniqueness.NODE_PATH);

        long numOfComputedSims = 0;
        int changeCounter1 = 0;     //folyamat kiiratásához

//        1 node teszteléséhez:
//        ArrayList<Node> nodeList = new ArrayList<>();
//        nodeList.add(graphDB.graphDBService.getNodeById(8));

        for (Node item : itemList) {
            int changes = computeCosineSimilarity(item, simNodeFinder, Relationships.HAS_META, simLinks);
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
        LogHelper.INSTANCE.log("Stop CBFSim!");

        if (uploadResultIntoDB) {
            LogHelper.INSTANCE.log("Upload computed similarities to DB:");
            graphDB.batchInsertSimilarities(simLinks, Similarities.CBF_SIM);
            LogHelper.INSTANCE.log("Upload computed similarities to DB Done!");
        }

        ArrayList<Double> vals = new ArrayList<>(simLinks.size());
        double sum = 0.0;
        for (SimLink s : simLinks) {
            sum += s.similarity;
            vals.add(s.similarity);
        }
        double avg = sum / simLinks.size();
        Collections.sort(vals);
        System.out.println("Max sim: " + vals.get(vals.size()-1));
        System.out.println("Min sim: " + vals.get(0));
        System.out.println("Avg sim: " + avg);
        System.out.println("Median sim: " + vals.get((vals.size()/2)));
        System.out.println("1st decile sim: " + vals.get((vals.size()/10)));
        System.out.println("2st decile sim: " + vals.get((vals.size()/10*2)));
    }

    public int computeCosineSimilarity(Node nodeA, TraversalDescription description,
                                       Relationships existingRelType, HashSet<SimLink<Long>> similarities) {

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
        ArrayList<SimLink> nodeSims = new ArrayList<>();
        while (friends.hasNext()) {
            friends.advance();
            if (friends.value() > 3.0) {      //csak azokat a hasonlóságokat számolom ki, ahol a suppAB > 1
                int suppB = friendNodes.get(friends.key());
                double sim = friends.value() / (Math.sqrt(suppA * suppB));
                nodeSims.add(new SimLink(startNodeID, friends.key(), sim));
//            System.out.println("A: " + suppA + " B: " + suppB + " AB: " + suppAB + " sim: " + sim);
            }
        }
        ArrayList<SimLink<Long>> simLinks = Ordering.from(this.getComparator()).greatestOf(nodeSims, 1000);
        for(SimLink<Long> s : simLinks){
            similarities.add(s);
            computedSims++;     //hány hasonlóságot tárolunk el
        }
        return computedSims;
    }

    public Comparator<SimLink<Long>> getComparator() {
        return (o1, o2) -> ((Double) o1.similarity).compareTo((Double) o2.similarity);
    }


//    private int lastUser = -1;
//    private int userRelDegree = 0;
//    private Node user = null;
//    private HashSet<Integer> itemsSeenByUser = new HashSet<>();
//    private int numUser = 0;

    /**
     * Prediktalasra. Arra felkeszitve, hogy a usereken megy sorba a kiertekeles, nem itemeken!
     *
     */
    public double predict(int uID, int iID, long time) {
        HashSet<String> itemWords = GraphDBBuilder.getUniqueItemMetaWordsByKey(this.db, iID, "VodMenuDirect", "[\f/]");
        long[] itemWordIDs = new long[itemWords.size()];
        int i = 0;
        for(String word : itemWords){
            itemWordIDs[i] = VOD_IDs.get(word);
            i++;
        }

        double prediction = 0.0;
//        if( uID != lastUser) {
//            itemsSeenByUser = userItems.get(uID);
//            userRelDegree = itemsSeenByUser.size();
//            lastUser = uID;
//            numUser++;
//            if(numUser % 1000 == 0)
//                System.out.println(numUser);
//        }



        return prediction;
    }

}
