package hf;

import gnu.trove.iterator.TObjectIntIterator;
import gnu.trove.map.hash.TObjectIntHashMap;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.TraversalDescription;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashSet;


public class CFGraphPredictor extends GraphDBPredictor {

    public void train(){
        super.train();
    }

    public void computeItemToItemSims() {

        LogHelper.INSTANCE.log("Start CFSim!");

        ArrayList<Node> nodeList = graphDB.getNodesByLabel(Labels.Item);

        Transaction tx = graphDB.graphDBService.beginTx();

        TraversalDescription simNodeFinder = graphDB.graphDBService.traversalDescription()
                .depthFirst()
                .relationships(Relationships.SEEN)
                .evaluator(Evaluators.atDepth(2));

        try {
            PrintWriter out = new PrintWriter("similarity.txt");

            int uncommittedChanges = 0;
            for (Node item : nodeList) {
                ArrayList<String> sims = computeCosineSimilarity(item, simNodeFinder, Relationships.SEEN, Similarities.CF_ISIM);
                uncommittedChanges = uncommittedChanges + sims.size();
                for (String s : sims) {
                    out.println(s);
                }

                if (uncommittedChanges > 1000) {
                    System.out.println("Commit");
                    uncommittedChanges = 0;
                    tx.success();
                    tx.close();
                    out.flush();
                    tx = graphDB.graphDBService.beginTx();
                }
            }
            tx.success();
            tx.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }


    }

    public ArrayList<String> computeCosineSimilarity(Node nodeA, TraversalDescription description,
                                       Relationships existingRelType, Similarities newRelType){
//        int changeCounter = 0;
        ArrayList<String> sims = new ArrayList<>();
        HashSet<Long> computedSimNodes = graphDB.getAllNeighborsBySim(nodeA,newRelType);   //a már kiszámolt item szomszédok
        TObjectIntHashMap<Node> friendNodes = new TObjectIntHashMap<>();
        TObjectIntHashMap<Long> suppABForAllB = new TObjectIntHashMap<>();

        int suppA = nodeA.getDegree(existingRelType);

        for(Path path : description.traverse(nodeA)) {
            Node friendNode = path.endNode();
            long id = friendNode.getId();
            if (!computedSimNodes.contains(id)) {                   //ha még nem lett kiszámolva a hasonlóságuk
                friendNodes.put(friendNode,friendNode.getDegree(existingRelType));      //suppB
                suppABForAllB.adjustOrPutValue(id,1,1);                    //suppAB növelés
            }
        }

        TObjectIntIterator<Node> friends = friendNodes.iterator();
        while(friends.hasNext()){
            friends.advance();
            Node friend = friends.key();
            int suppB = friends.value();
            double suppAB = (double) suppABForAllB.get(friend.getId());

            double sim = suppAB / (suppA * suppB);
            sims.add("" + nodeA.getId() + ";" + friend.getId() + ";" + sim);
//            System.out.println("A: " + suppA + " B: " + suppB + " AB: " + suppAB + " sim: " + sim);
//            Relationship newRel = nodeA.createRelationshipTo(friend,newRelType);
//            newRel.setProperty(newRelType.getPropertyName(),sim);
//            changeCounter++;
//            System.out.println(sim);

        }
//        System.out.println("Kész: " + nodeA.getProperty("id"));
//        return changeCounter;
        return sims;
    }

    public void test(){
    }
}
