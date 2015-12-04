package hf.GraphPredictors;

import gnu.trove.iterator.TLongIntIterator;
import gnu.trove.map.hash.TIntDoubleHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.map.hash.TLongIntHashMap;
import hf.GraphUtils.*;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.traversal.BranchState;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Uniqueness;

import java.util.*;


public class CFGraphPredictor extends GraphDBPredictor {
    private TIntObjectHashMap<TIntDoubleHashMap> itemSimilarities;
    private TIntObjectHashMap<HashSet<Integer>> userItems;
    private int method;
    private int minSuppAB;

    /**
     * @param graphDB A grafDB, amire epitkezik
     * @param method  Prediction szamitasi modszer 1-es: /összes, 2-es: /match
     */
    public void setParameters(GraphDB graphDB, Similarities sim, int method, int minSuppAB) {
        super.setParameters(graphDB);
        this.sim = sim;
        this.method = method;
        this.minSuppAB = minSuppAB;
    }

    public String getName(){
        return "CollabFill CoSim Predictor ";
    }

    public String getShortName(){
        return "CF_SIM";
    }

    @Override
    public void printParameters() {
        graphDB.printParameters();
        LogHelper.INSTANCE.logToFile(this.getName() + sim.name() + " Parameters:");
        LogHelper.INSTANCE.logToFile("Method: " + method);
        LogHelper.INSTANCE.logToFile("minSuppAB: " + minSuppAB);
    }

    public void trainFromGraphDB() {
        graphDB.initDB();
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

    public void computeSims(boolean uploadResultIntoDB) {
        LogHelper.INSTANCE.logToFileT("Start " + sim.name() + "!");

        HashSet<SimLink<Long>> simLinks = new HashSet<>();

        ArrayList<Node> nodeList = graphDB.getNodesByLabel(Labels.Item);

        Transaction tx = graphDB.startTransaction();

        LogHelper.INSTANCE.logToFileT("Item SEEN supportok számítása:");
        TLongIntHashMap nodeDegrees = new TLongIntHashMap(nodeList.size());    //friendNode_B --> suppB(SEEN)
        for (Node item : nodeList) {
            long nodeID = item.getId();
            int itemSeenDegree = this.graphDB.uniqueEvents ? item.getDegree() : GraphDB.getDistinctDegree(item, Relationships.SEEN);
            nodeDegrees.put(nodeID, itemSeenDegree);
        }
        LogHelper.INSTANCE.logToFileT("Item SEEN supportok számítása KÉSZ!");

        TraversalDescription simNodeFinder = getTraversalForActualEventList();


        long numOfComputedSims = 0;
        int changeCounter1 = 0;     //folyamat kiiratásához

//        1 node teszteléséhez:
//        ArrayList<Node> nodeList = new ArrayList<>();
//        nodeList.add(graphDB.graphDBService.getNodeById(5680));

        for (Node item : nodeList) {
            int changes = computeCosineSimilarity(item, simNodeFinder, simLinks, nodeDegrees);
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
    }

    public int computeCosineSimilarity(Node nodeA, TraversalDescription description,
                                       HashSet<SimLink<Long>> similarities, TLongIntHashMap nodeDegrees) {

        int computedSims = 0;

        TLongIntHashMap suppABForAllB = new TLongIntHashMap();  //friendNode_B --> suppAB

        long startNodeID = nodeA.getId();
        int suppA = nodeDegrees.get(startNodeID);

        for (Path path : description.traverse(nodeA)) {
            Node friendNode = path.endNode();
            long friendNodeId = friendNode.getId();
            if (!similarities.contains(new SimLink<>(startNodeID, friendNodeId))) {        //ha még nem lett kiszámolva a hasonlóságuk
                suppABForAllB.adjustOrPutValue(friendNodeId, 1, 1);                    //suppAB növelés
            }
        }

        TLongIntIterator friends = suppABForAllB.iterator();
        while (friends.hasNext()) {
            friends.advance();
            if (friends.value() > minSuppAB) {      //csak azokat a hasonlóságokat számolom ki, ahol a suppAB > 1
                computedSims++;     //hány hasonlóságot számoltunk ki
                int suppB = nodeDegrees.get(friends.key());
                double sim = friends.value() / (Math.sqrt(suppA * suppB));
                similarities.add(new SimLink<>(startNodeID, friends.key(), sim));
//            System.out.println("A: " + suppA + " B: " + suppB + " AB: " + suppAB + " sim: " + sim);
            }
        }
        return computedSims;
    }

    //Ha az event lista NEM unique! Alap grafbejarassal (nem AllEventes-sel).
    //A szurest itt vegzem el, nem a bejarasban ---> lassu!
    private int computeCoSimForAllEvents(Node nodeA, TraversalDescription description,
                                         Relationships existingRelType, HashSet<SimLink<Long>> similarities,
                                         TLongIntHashMap nodeDegrees) {

        int computedSims = 0;

        TLongIntHashMap friendNodes = new TLongIntHashMap();    //friendNode_B --> suppB
        TLongIntHashMap suppABForAllB = new TLongIntHashMap();  //friendNode_B --> suppAB

        long startNodeID = nodeA.getId();
        int suppA = getNodeDegreeFromMap(nodeA, startNodeID, nodeDegrees, existingRelType);

        HashSet<DirectedLink<Long>> paths = new HashSet<>();
        for (Path path : description.traverse(nodeA)) {
            Iterator<Node> pathNodes = path.nodes().iterator();
            pathNodes.next();           //kiindulasi pont
            Node user = pathNodes.next();       //user
            Node friendItem = pathNodes.next();     //masik item
            long userID = user.getId();
            long friendItemID = friendItem.getId();
            DirectedLink<Long> link = new DirectedLink<>(userID, friendItemID);
            if (!paths.contains(link)) {
                paths.add(link);
                if (!friendNodes.containsKey(friendItemID)) {
                    friendNodes.put(friendItemID, getNodeDegreeFromMap(friendItem, friendItemID, nodeDegrees, existingRelType));     //suppB
                }
                suppABForAllB.adjustOrPutValue(friendItemID, 1, 1);     //suppAB++
            }
        }

        TLongIntIterator friends = suppABForAllB.iterator();
        while (friends.hasNext()) {
            friends.advance();
            if (friends.value() > 1.0) {      //csak azokat a hasonlóságokat számolom ki, ahol a suppAB > 1
                computedSims++;     //hány hasonlóságot számoltunk ki
                int suppB = friendNodes.get(friends.key());
                double sim = friends.value() / (Math.sqrt(suppA * suppB));
                similarities.add(new SimLink<>(startNodeID, friends.key(), sim));
//            System.out.println("A: " + suppA + " B: " + suppB + " AB: " + suppAB + " sim: " + sim);
            }
        }
        return computedSims;
    }


    private TraversalDescription getTraversalForActualEventList() {
        return this.graphDB.uniqueEvents ?
                graphDB.graphDBService.traversalDescription()
                        .depthFirst()
                        .relationships(Relationships.SEEN)
                        .evaluator(Evaluators.atDepth(2))
                        .uniqueness(Uniqueness.RELATIONSHIP_PATH)
                        .uniqueness(Uniqueness.NODE_PATH)
                :
                //Megoldas a unique relaciok lekerese:
                graphDB.graphDBService.traversalDescription()
                        .depthFirst()
                        .expand(new PathExpander<Object>() {
                            @Override
                            public Iterable<Relationship> expand(Path path, BranchState<Object> objectBranchState) {
                                return GraphDB.getDistinctRelationships(path.endNode(), Relationships.SEEN);
                            }

                            @Override
                            public PathExpander<Object> reverse() {
                                return null;
                            }
                        })
                        .evaluator(Evaluators.atDepth(2))
                        .uniqueness(Uniqueness.RELATIONSHIP_PATH)
                        .uniqueness(Uniqueness.NODE_PATH);
    }

    private int lastUser = -1;
    private int userRelDegree = 0;
    private Node user = null;
    private HashSet<Integer> itemsSeenByUser = new HashSet<>();
    private int numUser = 0;

    /**
     * Prediktalasra. Arra felkeszitve, hogy a usereken megy sorba a kiertekeles, nem itemeken!
     *
     * @param uID userID
     * @param iID itemID
     * @return prediction
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


    private HashSet<Long> itemsSeenByUserDB = new HashSet<>();
    private HashMap<Long, Double> simsForItem = new HashMap<>();

    /**
     * Prediktalasra. Arra felkeszitve, hogy a usereken megy sorba a kiertekeles, nem itemeken!
     * GrafDB-bol keri le a user altal latott filmeket, adott itemhez hasonlosagi item listat
     * Tul sok disk muvelet -> lassu
     *
     * @param uID userID
     * @param iID itemID
     * @return prediction
     */
    public double predictFromDBDirectly(int uID, int iID, long time) {
        int matches = 0;
        double prediction = 0.0;
        if (uID != lastUser) {
            user = graphDB.graphDBService.findNode(Labels.User, Labels.User.getIDName(), uID);
            itemsSeenByUserDB = GraphDB.getAllNeighborDBIDsByRel(user, Relationships.SEEN);
            lastUser = uID;
            numUser++;
            if (numUser % 10 == 0)
                System.out.println(numUser);
        }

        simsForItem = GraphDB.getAllNeighborIDsBySim(graphDB.graphDBService.findNode(Labels.Item, Labels.Item.getIDName(),
                iID), Similarities.CF_ISIM);
        if (simsForItem.size() < itemsSeenByUser.size()) {
            for (Map.Entry<Long, Double> entry : simsForItem.entrySet()) {
                if (itemsSeenByUser.contains(entry.getKey())) {
                    prediction += entry.getValue();
                    matches++;
                }

            }
        } else {
            for (Long l : itemsSeenByUserDB) {
                if (simsForItem.containsKey(l)) {
                    prediction += simsForItem.get(l);
                    matches++;
                }

            }
        }

        if (method == 1) {
            int userRelDegree = user.getDegree(Relationships.SEEN);
            prediction = userRelDegree > 0 ? (prediction / userRelDegree) : 0.0;  //1-es módszer
        } else
            prediction = matches > 0 ? prediction / matches : 0.0;         //2-es módszer

        return prediction;
    }
}
