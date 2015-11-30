package hf.GraphPredictors;


import com.google.common.collect.Ordering;
import gnu.trove.iterator.TLongDoubleIterator;
import gnu.trove.map.hash.TIntDoubleHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.map.hash.TLongDoubleHashMap;
import gnu.trove.map.hash.TLongIntHashMap;
import hf.GraphUtils.*;
import onlab.core.Database;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Uniqueness;

import java.util.*;

public class WordCoSimGraphPredictor extends GraphDBPredictor {

    private TIntObjectHashMap<TIntDoubleHashMap> itemSimilarities;
    private TIntObjectHashMap<HashSet<Integer>> userItems;
    private int method;

    private HashMap<String, Double> metaWeights;
    private Relationships[] relTypes;

    /**
     *
     * @param graphDB GraphDB, amire epitkezik
     * @param db Teszteleshez kell a train db
     * @param sim Milyen similarity-t szamit ki
     * @param method Prediction szamitasi modszer 1-es: /összes, 2-es: /match
     * @param _weights Metaword sulyozas
     * @param rel_types Mely metawordoket hasznalja
     */
    public void setParameters(GraphDB graphDB, Database db, Similarities sim, int method,
                              HashMap<String,Double> _weights, Relationships[] rel_types){
        super.setParameters(graphDB,db);
        this.sim = sim;
        this.method = method;
        metaWeights = _weights;
        this.relTypes = rel_types;
    }

    @Override
    public void trainFromGraphDB() {
        LogHelper.INSTANCE.log("Adatok betöltése a gráfból:");
        LogHelper.INSTANCE.log("Similarity-k betöltése a gráfból:");
        itemSimilarities = graphDB.getAllSimilaritiesBySim(Labels.Item, sim);
        LogHelper.INSTANCE.log("Similarity-k betöltése a gráfból KÉSZ!");
        LogHelper.INSTANCE.log("Felhasználó-item kapcsolatok betöltése a gráfból:");
        userItems = graphDB.getAllUserItems();
        LogHelper.INSTANCE.log("Felhasználó-item kapcsolatok betöltése a gráfból KÉSZ! " + userItems.size() + " user betöltve!" );
        LogHelper.INSTANCE.log("Adatok betöltése a gráfból KÉSZ!");
        graphDB.shutDownDB();
    }


    /**
     * CosineSim alapu CBF
     * @param uploadResultIntoDB
     */
    public void computeSims(boolean uploadResultIntoDB) {
        LogHelper.INSTANCE.log("Start " + sim.name() + "!");

        Transaction tx = graphDB.startTransaction();
        ArrayList<Node> itemList = graphDB.getNodesByLabel(Labels.Item);

        HashSet<SimLink<Long>> simLinks = new HashSet<>();

        TraversalDescription simNodeFinder = graphDB.graphDBService.traversalDescription()
                .depthFirst()
                .relationships(Relationships.HAS_META)
                .relationships(Relationships.ACTS_IN)
                .relationships(Relationships.DIR_BY)
                .evaluator(Evaluators.atDepth(2))
                .uniqueness(Uniqueness.RELATIONSHIP_PATH)
                .uniqueness(Uniqueness.NODE_PATH);

        long numOfComputedSims = 0;
        int changeCounter1 = 0;     //folyamat kiiratásához

//        1 node teszteléséhez:
//        ArrayList<Node> nodeList = new ArrayList<>();
//        nodeList.add(graphDB.graphDBService.getNodeById(8));

        for (Node item : itemList) {
            int changes = computeCosineSimilarity(item, simNodeFinder, simLinks);
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
        LogHelper.INSTANCE.log("Stop " + sim.name() + "!");

        if (uploadResultIntoDB) {
            LogHelper.INSTANCE.log("Upload computed similarities to DB:");
            graphDB.batchInsertSimilarities(simLinks, sim);
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
                                       HashSet<SimLink<Long>> similarities) {

        int computedSims = 0;

        TLongIntHashMap friendNodes = new TLongIntHashMap();    //friendNode_B --> suppB
        TLongDoubleHashMap suppABForAllB = new TLongDoubleHashMap();  //friendNode_B --> suppAB

        long startNodeID = nodeA.getId();
        int suppA = GraphDB.getSumOfDegreesByRelationships(nodeA, relTypes);

        for (Path path : description.traverse(nodeA)) {
            Node friendNode = path.endNode();
            String rel = path.lastRelationship().getType().name();
            long friendNodeId = friendNode.getId();
            if (!similarities.contains(new SimLink(startNodeID, friendNodeId))) {        //ha még nem lett kiszámolva a hasonlóságuk
                if(!friendNodes.containsKey(friendNodeId))
                    friendNodes.put(friendNodeId,GraphDB.getSumOfDegreesByRelationships(friendNode,relTypes)); //suppB rel alapjan
                suppABForAllB.adjustOrPutValue(friendNodeId, 1, metaWeights.get(rel));                    //suppAB növelés
            }
        }

        TLongDoubleIterator friends = suppABForAllB.iterator();
        ArrayList nodeSims = new ArrayList<>();
        while (friends.hasNext()) {
            friends.advance();
            if (friends.value() > 3.0) {      //csak azokat a hasonlóságokat számolom ki, ahol a suppAB > 1
                int suppB = friendNodes.get(friends.key());
                double sim = friends.value() / (Math.sqrt(suppA * suppB));
                nodeSims.add(new SimLink(startNodeID, friends.key(), sim));
//            System.out.println("A: " + suppA + " B: " + suppB + " AB: " + suppAB + " sim: " + sim);
            }
        }
        List<SimLink<Long>> simLinks = Ordering.from(this.getComparator()).greatestOf(nodeSims, 1000);
        for(SimLink<Long> s : simLinks){
            similarities.add(s);
            computedSims++;     //hány hasonlóságot tárolunk el
        }
        return computedSims;
    }


    public Comparator<SimLink<Long>> getComparator() {
        return (o1, o2) -> ((Double) o1.similarity).compareTo((Double) o2.similarity);
    }


    private int lastUser = -1;
    private int userRelDegree = 0;
    private HashSet<Integer> itemsSeenByUser = new HashSet<>();
    private int numUser = 0;

    /**
     * Prediktalasra. Arra felkeszitve, hogy a usereken megy sorba a kiertekeles, nem itemeken!
     *
     */
    public double predict(int uID, int iID, long time) {

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
            itemsSeenByUser = userItems.get(uID);
            userRelDegree = itemsSeenByUser.size();
            lastUser = uID;
            numUser++;
            if(numUser % 1000 == 0)
                System.out.println(numUser);
        }

        for(int i : itemsSeenByUser) {
            double d;
            Link<Integer> l = new Link(i,iID);
            TIntDoubleHashMap itemSims = itemSimilarities.get(l.startNode);
            d = itemSims == null ? 0.0 : itemSims.get(l.endNode);
            if (d > 0.0) {
                prediction += d;
                matches++;
            }
        }

        if(method == 1) {
            prediction = userRelDegree > 0 ? (prediction / userRelDegree) : 0.0;  //1-es módszer
        }
        else
            prediction = matches > 0 ? prediction / matches : 0.0;         //2-es módszer

        return prediction;
    }

}
