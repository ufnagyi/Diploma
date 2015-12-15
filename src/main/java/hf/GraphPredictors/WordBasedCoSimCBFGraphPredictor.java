package hf.GraphPredictors;


import com.google.common.collect.Ordering;
import gnu.trove.iterator.TLongDoubleIterator;
import gnu.trove.map.hash.TIntDoubleHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.map.hash.TLongDoubleHashMap;
import gnu.trove.map.hash.TLongIntHashMap;
import hf.GraphUtils.*;
import onlab.core.Database;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.traversal.BranchState;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Uniqueness;

import java.util.*;

public class WordBasedCoSimCBFGraphPredictor extends GraphDBPredictor {
    private TIntObjectHashMap<TIntDoubleHashMap> itemSimilarities;
    private TIntObjectHashMap<HashSet<Integer>> userItems;
    private int method;
    private double minSuppAB;

    private HashMap<String, Double> metaWeights;
    private Relationships[] relTypes;

    /**
     * @param graphDB   GraphDB, amire epitkezik
     * @param sim       Milyen similarity-t szamit ki
     * @param method    Prediction szamitasi modszer 1-es: /összes, 2-es: /match
     * @param _weights  Metaword sulyozas
     * @param rel_types Mely metawordoket hasznalja
     */
    public void setParameters(GraphDB graphDB, Similarities sim, int method,
                              HashMap<String, Double> _weights, Relationships[] rel_types, double minSuppAB) {
        super.setParameters(graphDB);
        this.sim = sim;
        this.method = method;
        metaWeights = _weights;
        this.relTypes = rel_types;
        this.minSuppAB = minSuppAB;
    }

    public String getName() {
        return "Word Based CoSim Predictor ";
    }

    public String getShortName(){
        return "CBF_SIM";
    }

    @Override
    public void printParameters() {
        graphDB.printParameters();
        LogHelper.INSTANCE.logToFile(this.getName() + sim.name() + " Parameters:");
        LogHelper.INSTANCE.logToFile("Súlyok: " + metaWeights.toString());
        LogHelper.INSTANCE.logToFile("Relációk: " + Arrays.toString(relTypes));
        LogHelper.INSTANCE.logToFile("Method: " + method);
        LogHelper.INSTANCE.logToFile("MinSuppWordAB: " + minSuppAB);
    }

    @Override
    public void trainFromGraphDB() {
        graphDB.initDB();
        printParameters();
        LogHelper.INSTANCE.logToFileStartTimer("Adatok betöltése a gráfból:");
        LogHelper.INSTANCE.logToFileT("Similarity-k betöltése a gráfból:");
        itemSimilarities = graphDB.getAllSimilaritiesBySim(Labels.Item, sim);
        LogHelper.INSTANCE.logToFileT("Similarity-k betöltése a gráfból KÉSZ!");
        LogHelper.INSTANCE.logToFileT("Felhasználó-item kapcsolatok betöltése a gráfból:");
        userItems = graphDB.getAllUserItems();
        LogHelper.INSTANCE.logToFileT("Felhasználó-item kapcsolatok betöltése a gráfból KÉSZ! " + userItems.size() + " user betöltve!");
        LogHelper.INSTANCE.logToFileStopTimer("Adatok betöltése a gráfból KÉSZ!");
        graphDB.shutDownDB();
    }


    /**
     * CosineSim alapu CBF
     */
    public void computeSims(boolean uploadResultIntoDB) {
        LogHelper.INSTANCE.logToFileStartTimer("Start " + sim.name() + "!");

        ArrayList<Node> itemList = graphDB.getNodesByLabel(Labels.Item);

        HashSet<SimLink<Long>> simLinks = new HashSet<>();
        printParameters();
        if(!uploadResultIntoDB) {
            itemSimilarities = new TIntObjectHashMap<>();
        }

        Transaction tx = graphDB.startTransaction();

        LogHelper.INSTANCE.logToFileT("Item SEEN supportok számítása:");
        TLongIntHashMap nodeDegrees = new TLongIntHashMap(itemList.size());    //friendNode_B --> suppB(SEEN)
        TLongIntHashMap nodeItemIDs = new TLongIntHashMap(itemList.size());     //long id -> int id
        for (Node item : itemList) {
            long nodeID = item.getId();
            int itemWordDegree = GraphDB.getSumOfDegreesByRelationships(item, relTypes);
            nodeDegrees.put(nodeID, itemWordDegree);
            if(!uploadResultIntoDB){
                int itemID = (int)item.getProperty(Labels.Item.getIDName());
                nodeItemIDs.put(nodeID,itemID);
                TIntDoubleHashMap tIntDoubleHashMap = new TIntDoubleHashMap();
                itemSimilarities.put(itemID,tIntDoubleHashMap);
            }
        }
        LogHelper.INSTANCE.logToFileT("Item SEEN supportok számítása KÉSZ!");

        TraversalDescription simNodeFinder = graphDB.graphDBService.traversalDescription()
                .depthFirst()
                .expand(new PathExpander<Object>() {
                    @Override
                    public Iterable<Relationship> expand(Path path, BranchState<Object> objectBranchState) {
                        return path.endNode().getRelationships(relTypes);
                    }
                    @Override
                    public PathExpander<Object> reverse() {
                        return null;
                    }
                })
                .evaluator(Evaluators.atDepth(2))
                .uniqueness(Uniqueness.RELATIONSHIP_PATH);  //mivel metaadatokra nincs relacioismetles,
        // így node uniqunessre nincs szukseg!

        long numOfComputedSims = 0;
        int changeCounter1 = 0;     //folyamat kiiratásához

//        1 node teszteléséhez:
//        ArrayList<Node> nodeList = new ArrayList<>();
//        nodeList.add(graphDB.graphDBService.getNodeById(8));

        for (Node item : itemList) {
            int changes = computeCosineSimilarity(item, simNodeFinder, simLinks, nodeDegrees, uploadResultIntoDB, nodeItemIDs);
            changeCounter1 += changes;
            if (changeCounter1 > 50000) {
                graphDB.endTransaction(tx);
                tx = graphDB.startTransaction();
                numOfComputedSims += changeCounter1;
                changeCounter1 = 0;
                System.out.println(numOfComputedSims);
            }
        }
        graphDB.endTransaction(tx);
        printComputedSimilarityResults(simLinks, uploadResultIntoDB);
        if(!uploadResultIntoDB){
            LogHelper.INSTANCE.logToFileT("Felhasználó-item kapcsolatok betöltése a gráfból:");
            userItems = graphDB.getAllUserItems();
            LogHelper.INSTANCE.logToFileT("Felhasználó-item kapcsolatok betöltése a gráfból KÉSZ!");
        }
        graphDB.shutDownDB();
    }

    public int computeCosineSimilarity(Node nodeA, TraversalDescription description,
                                       HashSet<SimLink<Long>> similarities, TLongIntHashMap nodeDegrees,
                                       boolean uploadResultIntoDB, TLongIntHashMap nodeItemIDs) {

        int computedSims = 0;

        TLongDoubleHashMap suppABForAllB = new TLongDoubleHashMap();  //friendNode_B --> suppAB

        long startNodeID = nodeA.getId();
        int suppA = nodeDegrees.get(startNodeID);

        for (Path path : description.traverse(nodeA)) {
            Node friendNode = path.endNode();
            long friendNodeId = friendNode.getId();
            if (!similarities.contains(new SimLink<>(startNodeID, friendNodeId))) {        //ha még nem lett kiszámolva a hasonlóságuk
                String rel = path.lastRelationship().getType().name();
                suppABForAllB.adjustOrPutValue(friendNodeId, metaWeights.get(rel), metaWeights.get(rel));                    //suppAB növelés
            }
        }

        TLongDoubleIterator friends = suppABForAllB.iterator();
        ArrayList nodeSims = new ArrayList<>();
        while (friends.hasNext()) {
            friends.advance();
            if (friends.value() > minSuppAB) {      //csak azokat a hasonlóságokat számolom ki, ahol a suppAB > 1
                long friendNodeID = friends.key();
                int suppB = nodeDegrees.get(friendNodeID);
                double sim = friends.value() / (Math.sqrt(suppA * suppB));
                nodeSims.add(new SimLink<>(startNodeID, friendNodeID, sim));
//            System.out.println("A: " + suppA + " B: " + suppB + " AB: " + suppAB + " sim: " + sim);
            }
        }
        List<SimLink<Long>> simLinks = Ordering.from(SimLink.getComparator()).greatestOf(nodeSims, 1000);
        for (SimLink<Long> s : simLinks) {
            similarities.add(s);
            if(!uploadResultIntoDB){
                itemSimilarities.get(nodeItemIDs.get(s.startNode)).put(nodeItemIDs.get(s.endNode),s.similarity);
            }
            computedSims++;     //hány hasonlóságot tárolunk el
        }
        return computedSims;
    }

    public void setMethod(int m){
        this.method = m;
        resetNumUser();
    }

    public void resetNumUser(){
        this.numUser = 0;
    }


    private int lastUser = -1;
    private int userRelDegree = 0;
    private HashSet<Integer> itemsSeenByUser = new HashSet<>();
    private int numUser = 0;

    /**
     * Prediktalasra. Arra felkeszitve, hogy a usereken megy sorba a kiertekeles, nem itemeken!
     */
    public double predict(int uID, int iID, long time) {

        /*
        Kétféle megközelítés:
        A vizsgált item és a user által látott egyes itemek között levő sim értékek szummáját:
        1) a User által látott összes film számával átlagolom --> prediction / user.getDegree(Relationships.SEEN)
        2) a User által látott filmek közül az adott itemmel sim kapcsolatban állók számával átlagolom
            --> prediction / matches
         */
        int matches = 0;
        double prediction = 0.0;
        if (uID != lastUser) {
            itemsSeenByUser = userItems.get(uID);
            userRelDegree = itemsSeenByUser.size();
            lastUser = uID;
            numUser++;
            if (numUser % 1000 == 0)
                System.out.println(numUser);
        }

        for (int i : itemsSeenByUser) {
            double d;
            Link<Integer> l = new Link<>(i, iID);
            TIntDoubleHashMap itemSims = itemSimilarities.get(l.startNode);
            d = itemSims == null ? 0.0 : itemSims.get(l.endNode);
            if (d > 0.0) {
                prediction += d;
                matches++;
            }
        }

        if (method == 1) {
            prediction = userRelDegree > 0 ? (prediction / userRelDegree) : 0.0;  //1-es módszer
        } else
            prediction = matches > 0 ? prediction / matches : 0.0;         //2-es módszer

        return prediction;
    }

}