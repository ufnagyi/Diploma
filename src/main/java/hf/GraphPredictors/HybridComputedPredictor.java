package hf.GraphPredictors;


import com.google.common.collect.Ordering;
import gnu.trove.iterator.TLongDoubleIterator;
import gnu.trove.iterator.TLongIntIterator;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.map.hash.TIntDoubleHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.map.hash.TLongDoubleHashMap;
import gnu.trove.map.hash.TLongIntHashMap;
import gnu.trove.set.hash.TLongHashSet;
import hf.GraphUtils.*;
import onlab.core.Database;
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
    private boolean uniqueEvents;

    public void setParameters(GraphDB graphDB, Database db, Similarities sim, int method, boolean uniqueEvents_,
                              HashMap<String,Double> _weights, Relationships[] rel_types){
        super.setParameters(graphDB,db);
        this.sim = sim;
        this.method = method;
        this.uniqueEvents = uniqueEvents_;
        this.metaWeights = _weights;
        this.relTypes = rel_types;
    }

    @Override
    public void trainFromGraphDB() {
        graphDB.initDB();
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

    @Override
    protected void computeSims(boolean uploadResultIntoDB) {
        LogHelper.INSTANCE.log("Start " + sim.name() + "!");

        HashSet<SimLink<Long>> simLinks = new HashSet<>();

        ArrayList<Node> nodeList = graphDB.getNodesByLabel(Labels.Item);

        Transaction tx = graphDB.startTransaction();

        TraversalDescription simNodeFinder = getTraversalForActualEventList();

        LogHelper.INSTANCE.log("Item word és SEEN supportok számítása:");
        TLongIntHashMap nodeWordDegrees = new TLongIntHashMap(nodeList.size());    //friendNode_B --> suppB(RelTypes)
        TLongIntHashMap nodeSeenDegrees = new TLongIntHashMap(nodeList.size());    //friendNode_B --> suppB(SEEN)
        for(Node item : nodeList) {
            long nodeID = item.getId();
            int itemSeenDegree = uniqueEvents ? item.getDegree() : GraphDB.getDistinctDegree(item, Relationships.SEEN);
            nodeSeenDegrees.put(nodeID, itemSeenDegree);
            nodeWordDegrees.put(nodeID, GraphDB.getSumOfDegreesByRelationships(item, relTypes));
        }
        LogHelper.INSTANCE.log("Item word és SEEN supportok számítása KÉSZ!");

        long numOfComputedSims = 0;
        int changeCounter1 = 0;     //folyamat kiiratásához

//        1 node teszteléséhez:
//        ArrayList<Node> nodeList = new ArrayList<>();
//        nodeList.add(graphDB.graphDBService.getNodeById(5680));

        for (Node item : nodeList) {
            int changes = computeCosineSimilarityForUniqueEvents(item, simNodeFinder, simLinks,
                    nodeSeenDegrees, nodeWordDegrees);
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
        printComputedSimilarityResults(simLinks,uploadResultIntoDB);
    }

    private int computeCosineSimilarityForUniqueEvents(Node nodeA, TraversalDescription description,
                                                       HashSet<SimLink<Long>> similarities,
                                                       TLongIntHashMap nodeSeenDegrees,
                                                       TLongIntHashMap nodeWordDegrees) {

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
                    suppWordABForAllB.adjustOrPutValue(friendNodeId, 1, metaWeights.get(relName));
            }
        }

        ArrayList nodeSims = new ArrayList<>(allFriendNodes.size());

        TLongIterator allFriends = allFriendNodes.iterator();
        while (allFriends.hasNext()) {
            long friendNodeId = allFriends.next();
            int suppSeenAB = suppSeenABForAllB.get(friendNodeId);
            suppSeenAB = (suppSeenAB > 1) ? suppSeenAB : 0; //csak azokat a CF hasonlosagokat szamolom, ahol a suppAB > 1
            double suppWordAB = suppWordABForAllB.get(friendNodeId);
            suppWordAB = (suppWordAB > 4.0) ? suppWordAB : 0; //csak azokat a CBF hasonlosagokat szamolom, ahol a suppAB > 4
            int suppSeenB = nodeSeenDegrees.get(friendNodeId);
            int suppWordB = nodeWordDegrees.get(friendNodeId);
            double sim = (suppSeenAB + suppWordAB) / (Math.sqrt(suppSeenA * suppSeenB) + Math.sqrt(suppWordA * suppWordB));
            similarities.add(new SimLink<>(startNodeID, friendNodeId, sim));
//            System.out.println("A: " + suppA + " B: " + suppB + " AB: " + suppAB + " sim: " + sim);
        }
        List<SimLink<Long>> simLinks = Ordering.from(SimLink.getComparator()).greatestOf(nodeSims, 1000);
        for(SimLink<Long> s : simLinks){
            similarities.add(s);
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
        return (uniqueEvents) ?
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
                //Megoldas a unique relaciok lekerese:
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
                                    relShips = GraphDB.getDistinctRelationships(path.endNode(), Relationships.SEEN);
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
        int matches =  0;

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
            Link<Integer> l = new Link<>(i,iID);
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
