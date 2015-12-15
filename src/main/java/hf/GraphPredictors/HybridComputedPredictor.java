package hf.GraphPredictors;


import com.google.common.collect.Ordering;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.map.hash.TIntDoubleHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.map.hash.TLongDoubleHashMap;
import gnu.trove.map.hash.TLongIntHashMap;
import gnu.trove.set.hash.TLongHashSet;
import hf.GraphUtils.*;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.traversal.BranchState;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Uniqueness;

import java.util.*;

public class HybridComputedPredictor extends GraphDBPredictor {
    private TIntObjectHashMap<TIntDoubleHashMap> itemSimilarities;
    private TIntObjectHashMap<HashSet<Integer>> userItems;
    private HashMap<String, Double> metaWeights;
    private Relationships[] relTypes;
    private int method;
    private int minSuppSeenAB;
    private double minSuppWordAB;
    private double CF_SIM_WEIGHT;    //hogyan atlagoljam a ket alg eredmenyet egybe
    private double CBF_SIM_WEIGHT;

    public void setParameters(GraphDB graphDB, Similarities sim, int method,
                              HashMap<String, Double> _weights, Relationships[] rel_types,
                              double minSuppWordAB, int minSuppSeenAB, double _cf_sim_w) {
        super.setParameters(graphDB);
        this.sim = sim;
        this.method = method;
        this.metaWeights = _weights;
        this.relTypes = rel_types;
        this.minSuppSeenAB = minSuppSeenAB;
        this.minSuppWordAB = minSuppWordAB;
        this.CF_SIM_WEIGHT = _cf_sim_w;
        this.CBF_SIM_WEIGHT = 1.0 - CF_SIM_WEIGHT;
    }

    public String getName() {
        return "Hybrid Computed Predictor ";
    }

    public String getShortName(){
        return "HYB_COMPUTED";
    }

    public void printParameters() {
        graphDB.printParameters();
        LogHelper.INSTANCE.logToFile(this.getName() + sim.name() + " Parameters:");
        LogHelper.INSTANCE.logToFile("Súlyok: " + metaWeights.toString());
        LogHelper.INSTANCE.logToFile("Relációk: " + Arrays.toString(relTypes));
        LogHelper.INSTANCE.logToFile("Method: " + method);
        LogHelper.INSTANCE.logToFile("CF_SIM_WEIGHT: " + CF_SIM_WEIGHT);
        LogHelper.INSTANCE.logToFile("MinSuppWordAB: " + minSuppWordAB);
        LogHelper.INSTANCE.logToFile("MinSuppSeenAB: " + minSuppSeenAB);
    }

    @Override
    public void trainFromGraphDB() {
        graphDB.initDB();
        printParameters();
        LogHelper.INSTANCE.logToFileT("Adatok betöltése a gráfból:");
        LogHelper.INSTANCE.logToFileT("Similarity-k betöltése a gráfból:");
        itemSimilarities = graphDB.getAllSimilaritiesBySim(Labels.Item, sim);
        LogHelper.INSTANCE.logToFileT("Similarity-k betöltése a gráfból KÉSZ!");
        LogHelper.INSTANCE.logToFileT("Felhasználó-item kapcsolatok betöltése a gráfból:");
        userItems = graphDB.getAllUserItems();
        LogHelper.INSTANCE.logToFileT("Felhasználó-item kapcsolatok betöltése a gráfból KÉSZ! " + userItems.size() + " user betöltve!");
        LogHelper.INSTANCE.logToFileT("Adatok betöltése a gráfból KÉSZ!");
        graphDB.shutDownDB();
    }

    @Override
    protected void computeSims(boolean uploadResultIntoDB) {
        LogHelper.INSTANCE.logToFileStartTimer("Start " + sim.name() + "!");

        HashSet<SimLink<Long>> simLinks = new HashSet<>();

        printParameters();
        if(!uploadResultIntoDB) {
            itemSimilarities = new TIntObjectHashMap<>();
        }
        ArrayList<Node> nodeList = graphDB.getNodesByLabel(Labels.Item);

        Transaction tx = graphDB.startTransaction();

        TraversalDescription simNodeFinder = getTraversalForActualEventList();

        LogHelper.INSTANCE.logToFileT("Item word és SEEN supportok számítása:");
        TLongIntHashMap nodeWordDegrees = new TLongIntHashMap(nodeList.size());    //friendNode_B --> suppB(RelTypes)
        TLongIntHashMap nodeSeenDegrees = new TLongIntHashMap(nodeList.size());    //friendNode_B --> suppB(SEEN)
        TLongIntHashMap nodeItemIDs = new TLongIntHashMap(nodeList.size());     //long id -> int id
        for (Node item : nodeList) {
            long nodeID = item.getId();
            int itemSeenDegree = this.graphDB.uniqueEvents ? item.getDegree() : GraphDB.getDistinctDegree(item, Relationships.SEEN);
            nodeSeenDegrees.put(nodeID, itemSeenDegree);
            nodeWordDegrees.put(nodeID, GraphDB.getSumOfDegreesByRelationships(item, relTypes));
            if(!uploadResultIntoDB){
                int itemID = (int)item.getProperty(Labels.Item.getIDName());
                nodeItemIDs.put(nodeID,itemID);
                TIntDoubleHashMap tIntDoubleHashMap = new TIntDoubleHashMap();
                itemSimilarities.put(itemID,tIntDoubleHashMap);
            }
        }
        LogHelper.INSTANCE.logToFileT("Item word és SEEN supportok számítása KÉSZ!");

        long numOfComputedSims = 0;
        int changeCounter1 = 0;     //folyamat kiiratásához

        //        1 node teszteléséhez:
//        nodeList = new ArrayList<>();
//        nodeList.add(graphDB.graphDBService.getNodeById(5680));

        for (Node item : nodeList) {
            int changes = computeCosineSimilarity(item, simNodeFinder, simLinks,
                    nodeSeenDegrees, nodeWordDegrees, uploadResultIntoDB, nodeItemIDs);
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

    private int computeCosineSimilarity(Node nodeA, TraversalDescription description,
                                        HashSet<SimLink<Long>> similarities,
                                        TLongIntHashMap nodeSeenDegrees, TLongIntHashMap nodeWordDegrees,
                                        boolean uploadResultIntoDB, TLongIntHashMap nodeItemIDs) {

        int computedSims = 0;
        long startNodeID = nodeA.getId();
        int suppSeenA = nodeSeenDegrees.get(startNodeID);
        int suppWordA = nodeWordDegrees.get(startNodeID);

        TLongIntHashMap suppSeenABForAllB = new TLongIntHashMap();  //friendNode_B --> suppAB
        TLongDoubleHashMap suppWordABForAllB = new TLongDoubleHashMap();  //friendNode_B --> suppAB
        TLongHashSet allFriendNodes = new TLongHashSet();       //Az osszes wordon vagy useren at elert friend Itemek

        for (Path path : description.traverse(nodeA)) {
            Node friendNode = path.endNode();
            long friendNodeId = friendNode.getId();
            if (!similarities.contains(new SimLink<>(startNodeID, friendNodeId))) {        //ha még nem lett kiszámolva a hasonlóságuk
                allFriendNodes.add(friendNodeId);
                String relName = path.lastRelationship().getType().name();
                if (relName.equals(Relationships.SEEN.name()))
                    suppSeenABForAllB.adjustOrPutValue(friendNodeId, 1, 1);                    //suppAB növelés
                else
                    suppWordABForAllB.adjustOrPutValue(friendNodeId, metaWeights.get(relName), metaWeights.get(relName));
            }
        }

        ArrayList nodeSims = new ArrayList<>(allFriendNodes.size());

        TLongIterator allFriends = allFriendNodes.iterator();
        while (allFriends.hasNext()) {
            long friendNodeId = allFriends.next();
            int suppSeenAB = suppSeenABForAllB.get(friendNodeId);
            suppSeenAB = (suppSeenAB > minSuppSeenAB) ? suppSeenAB : 0; //csak azokat a CF hasonlosagokat szamolom, ahol a suppAB > minSuppSeenAB
            double suppWordAB = suppWordABForAllB.get(friendNodeId);
            suppWordAB = (suppWordAB > minSuppWordAB) ? suppWordAB : 0; //csak azokat a CBF hasonlosagokat szamolom, ahol a suppAB > minSuppWordAB
            int suppSeenB = nodeSeenDegrees.get(friendNodeId);
            int suppWordB = nodeWordDegrees.get(friendNodeId);
            double metaSuppDenominator = Math.sqrt(suppWordA * suppWordB);
            double seenSuppDenominator = Math.sqrt(suppSeenA * suppSeenB);
            double equalizerWeight = (seenSuppDenominator > 0) ? seenSuppDenominator / metaSuppDenominator : 1.0;

            double sim = (CF_SIM_WEIGHT * suppSeenAB + CBF_SIM_WEIGHT * equalizerWeight * suppWordAB) / (CF_SIM_WEIGHT * seenSuppDenominator + CBF_SIM_WEIGHT * equalizerWeight * metaSuppDenominator);
            if (sim > 0.0)
                nodeSims.add(new SimLink<>(startNodeID, friendNodeId, sim));
//            LogHelper.INSTANCE.logToFile("A: " + suppA + " B: " + suppB + " AB: " + suppAB + " sim: " + sim);
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

    private TraversalDescription getTraversalForActualEventList() {
        Relationships[] allRelevantRelationships = new Relationships[relTypes.length + 1];
        int i = 0;
        for (Relationships r : relTypes) {
            allRelevantRelationships[i] = r;
            i++;
        }
        allRelevantRelationships[i] = Relationships.SEEN;
        return (this.graphDB.uniqueEvents) ?
                graphDB.graphDBService.traversalDescription()
                        .depthFirst()
                        .expand(new PathExpander<Object>() {
                            @Override
                            public Iterable<Relationship> expand(Path path, BranchState<Object> branchState) {
                                return path.endNode().getRelationships(allRelevantRelationships);
                            }

                            @Override
                            public PathExpander<Object> reverse() {
                                return null;
                            }
                        })
                        .evaluator(Evaluators.atDepth(2))
                        .uniqueness(Uniqueness.RELATIONSHIP_PATH)
                :
                //Megoldas AllEvents eseten relaciok lekeresere:
                graphDB.graphDBService.traversalDescription()
                        .depthFirst()
                        .expand(new PathExpander<Object>() {
                            @Override
                            public Iterable<Relationship> expand(Path path, BranchState<Object> objectBranchState) {
                                int depth = path.length();
                                Node pathEndNode = path.endNode();
                                if (depth == 0) {
                                    ArrayList<Relationship> relationships = new ArrayList<>();
                                    Iterable<Relationship> relShips = pathEndNode.getRelationships(relTypes);
                                    Iterator<Relationship> iterator = relShips.iterator();
                                    while (iterator.hasNext())
                                        relationships.add(iterator.next());
                                    relShips = GraphDB.getDistinctRelationships(pathEndNode, Relationships.SEEN);
                                    iterator = relShips.iterator();
                                    while (iterator.hasNext())
                                        relationships.add(iterator.next());
                                    return relationships;
                                } else {
                                    if (path.lastRelationship().getType().name().equals("SEEN"))
                                        return GraphDB.getDistinctRelationships(pathEndNode, Relationships.SEEN);
                                    else
                                        return pathEndNode.getRelationships(relTypes);
                                }
                            }

                            @Override
                            public PathExpander<Object> reverse() {
                                return null;
                            }
                        })
                        .evaluator(Evaluators.atDepth(2))
                        .uniqueness(Uniqueness.NODE_PATH);
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

    @Override
    public double predict(int uID, int iID, long time) {
        double prediction = 0.0;
       /*
        Kétféle megközelítés:
        A vizsgált item és a user által látott egyes itemek között levő sim értékek szummáját:
        1) a User által látott összes film számával átlagolom --> prediction / user.getDegree(Relationships.SEEN)
        2) a User által látott filmek közül az adott itemmel sim kapcsolatban állók számával átlagolom
            --> prediction / matches
         */
        int matches = 0;

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
