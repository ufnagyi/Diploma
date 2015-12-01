package hf.GraphPredictors;


import com.google.common.collect.HashBiMap;
import gnu.trove.iterator.TLongIntIterator;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TIntIntHashMap;
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

public class UserProfileBasedCoSimCBFPredictor extends GraphDBPredictor {
    private HashBiMap<String, Long> metaIDs;
    private TIntObjectHashMap<TLongIntHashMap> userProfiles;
    private TIntIntHashMap userProfileSupports;
    private Relationships[] relTypes;
    private Labels[] labelTypes;
    private String[] keyValueTypes;


    public void setParameters(GraphDB graphDB, Database db,
                              Relationships[] rel_types, Labels[] labelTypes,
                              String[] keyValueTypes_){
        super.setParameters(graphDB,db);
        this.relTypes = rel_types;
        this.keyValueTypes = keyValueTypes_;
        this.labelTypes = labelTypes;
    }

    protected void computeSims(boolean uploadResultIntoDB){
        populateMetaIDs();
        buildUserProfiles();
    }

    public void trainFromGraphDB() {
        this.computeSims(false);
    }

    /**
     * String word -> long ID
     */
    private void populateMetaIDs(){
        LogHelper.INSTANCE.log("MetaID map előállítása:");
        metaIDs = graphDB.getMetaIDs(labelTypes);
        LogHelper.INSTANCE.log("MetaID map előállítása KÉSZ!");
    }


    private void buildUserProfiles(){
        LogHelper.INSTANCE.log("Start CoSimCBF with UserProfile:");
        Transaction tx = graphDB.startTransaction();
        ArrayList<Node> userList = graphDB.getNodesByLabel(Labels.User);

//        1 node teszteléséhez:
//        ArrayList<Node> userList = new ArrayList<>();
//        userList.add(graphDB.graphDBService.getNodeById(17020));


        userProfiles = new TIntObjectHashMap<>(userList.size());
        userProfileSupports = new TIntIntHashMap(userList.size());

        TraversalDescription mWordsOfItemsSeenByUser = getNewMetaTraversalByUser(graphDB,relTypes);

        int numOfComputedUserProfiles = 0;
        int changeCounter1 = 0;

        for (Node user : userList) {
            computeProfile(user, mWordsOfItemsSeenByUser);
            changeCounter1++;
            if(changeCounter1 > 30000){
                graphDB.endTransaction(tx);
                tx = graphDB.startTransaction();
                numOfComputedUserProfiles += changeCounter1;
                changeCounter1 = 0;
                System.out.println(numOfComputedUserProfiles);
            }
        }
        numOfComputedUserProfiles += changeCounter1;

        graphDB.endTransaction(tx);
        LogHelper.INSTANCE.log("CoSimCBF with UserProfile KÉSZ! " + numOfComputedUserProfiles);
    }

    private void computeProfile(Node user, TraversalDescription description){
        int userID = (int) user.getProperty(Labels.User.getIDName());
        int userProfileSupp = 0;
        TLongIntHashMap mWordFrequencies = new TLongIntHashMap();
        for (Path path : description.traverse(user)) {
            mWordFrequencies.adjustOrPutValue(path.endNode().getId(),1,1);
            userProfileSupp++;
        }
        userProfiles.put(userID,mWordFrequencies);
        userProfileSupports.put(userID,userProfileSupp);
    }

    public static TraversalDescription getNewMetaTraversalByUser(GraphDB graphDB_, Relationships[] relTypes_) {
        return graphDB_.graphDBService.traversalDescription()
                .depthFirst()
                .expand(new PathExpander<Object>() {
                    @Override
                    public Iterable<Relationship> expand(Path path, BranchState<Object> objectBranchState) {
                        int depth = path.length();
                        if (depth == 0) {
                            return path.endNode().getRelationships(
                                    Relationships.SEEN);
                        } else {
                            return path.endNode().getRelationships(relTypes_);
                        }
                    }
                    @Override
                    public PathExpander<Object> reverse() {
                        return null;
                    }
                })
                .evaluator(Evaluators.atDepth(2))
                .uniqueness(Uniqueness.RELATIONSHIP_PATH);
    }


    private int lastUser = -1;
    private int userRelDegree = 0;
    private TLongIntHashMap userProfile = new TLongIntHashMap();
    private int numUser = 0;

    /**
     * Prediktalasra. Arra felkeszitve, hogy a usereken megy sorba a kiertekeles, nem itemeken!
     * A user profiljaban es az itemnel is szereplo metawordok tfidf sulyanak atlaga
     * 1-es method: a userprofile.size-zal atlagolva
     * 2-es method: csak a matchelt metawordok szamaval atlagolva
     */
    public double predict(int uID, int iID, long time) {
        double prediction = 0.0;

        TLongArrayList itemMetaWordIDs = new TLongArrayList();
        int i = 0;
        for (String keyValue : keyValueTypes) {
            HashSet<String> itemWords = GraphDBBuilder.getUniqueItemMetaWordsByKey(this.db, iID, keyValue);
            for (String word : itemWords) {
                Long metaID = metaIDs.get(i + "" + word);
                if(metaID != null)
                    itemMetaWordIDs.add(metaID.longValue());
            }
            i++;
        }

        if( uID != lastUser) {
            userProfile = userProfiles.get(uID);
            userRelDegree = userProfileSupports.get(uID);
            lastUser = uID;
            numUser++;
            if(numUser % 1000 == 0)
                System.out.println(numUser);
        }

        int matches = 0;

        TLongIterator itemMetaWords = itemMetaWordIDs.iterator();
        while(itemMetaWords.hasNext()){
            long word = itemMetaWords.next();
            matches += userProfile.get(word);
        }

        prediction = userRelDegree > 0 ? ((double) matches / userRelDegree) : 0.0;

        return prediction;
    }
}
