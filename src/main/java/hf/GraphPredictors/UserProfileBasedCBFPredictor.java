package hf.GraphPredictors;


import com.google.common.collect.HashBiMap;
import gnu.trove.iterator.TLongIntIterator;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.*;
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

public class UserProfileBasedCBFPredictor extends GraphDBPredictor {
    private HashBiMap<String, Long> metaIDs;
    private TIntObjectHashMap<TLongDoubleHashMap> userProfiles;
    private HashMap<String, Double> metaWeights;
    private Relationships[] relTypes;
    private Labels[] labelTypes;
    private String[] keyValueTypes;
    private TLongDoubleHashMap wordIDFs;
    int method;


    public void setParameters(GraphDB graphDB, Database db, int method,
                              HashMap<String,Double> _weights, Relationships[] rel_types,
                              String[] keyValueTypes_, Labels[] labelTypes){
        super.setParameters(graphDB,db);
        this.method = method;
        metaWeights = _weights;
        this.relTypes = rel_types;
        this.keyValueTypes = keyValueTypes_;
        this.labelTypes = labelTypes;
    }

    protected void computeSims(boolean uploadResultIntoDB){
        for(int i = 0; i < labelTypes.length; i++)
            graphDB.computeAndUploadIDFValues(labelTypes[i],relTypes[i]);
        populateMetaIDs();
        buildUserProfiles();
    }

    public void trainFromGraphDB(){
        populateMetaIDs();
        buildUserProfiles();
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
        LogHelper.INSTANCE.log("Start CBFSim with UserProfile:");
        Transaction tx = graphDB.startTransaction();
        ArrayList<Node> userList = graphDB.getNodesByLabel(Labels.User);

//        1 node teszteléséhez:
//        ArrayList<Node> userList = new ArrayList<>();
//        userList.add(graphDB.graphDBService.getNodeById(17020));


        // word_IDF = numOfItems / numOfItemsContainingTheWord
        wordIDFs = graphDB.getMetaIDFValues(labelTypes);

        userProfiles = new TIntObjectHashMap<>(userList.size());

        TraversalDescription mWordsOfItemsSeenByUser = graphDB.graphDBService.traversalDescription()
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
                            return path.endNode().getRelationships(relTypes);
                        }
                    }
                    @Override
                    public PathExpander<Object> reverse() {
                        return null;
                    }
                })
                .evaluator( Evaluators.atDepth( 2 ))
                .uniqueness(Uniqueness.RELATIONSHIP_PATH);



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
        LogHelper.INSTANCE.log("CBFSim with UserProfile KÉSZ! " + numOfComputedUserProfiles);
    }

    private void computeProfile(Node user, TraversalDescription description){

        int userID = (int) user.getProperty(Labels.User.getIDName());
        TLongIntHashMap mWordFrequencies = new TLongIntHashMap();
        int numOfMetaWords = 0;

        for (Path path : description.traverse(user)) {
            Node mWord = path.endNode();
            mWordFrequencies.adjustOrPutValue(mWord.getId(),1,1);
            numOfMetaWords++;
//            System.out.println(mWord.getAllProperties());
        }
        TLongDoubleHashMap userProfile = new TLongDoubleHashMap(mWordFrequencies.size());
        TLongIntIterator mWordFrequency = mWordFrequencies.iterator();
        while(mWordFrequency.hasNext()){
            mWordFrequency.advance();
            long metaWord = mWordFrequency.key();
            int wordFrequency = mWordFrequency.value();
//            System.out.println(metaIDs.inverse().get(metaWord) + ": " + wordFrequency);
            double relativeWordFrequency = (double) wordFrequency / numOfMetaWords;
            double tf_iDF = relativeWordFrequency * wordIDFs.get(metaWord);
            userProfile.put(metaWord,tf_iDF);
        }
        userProfiles.put(userID,userProfile);
    }




    private int lastUser = -1;
    private int userRelDegree = 0;
    private TLongDoubleHashMap userProfile = new TLongDoubleHashMap();
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
                itemMetaWordIDs.add(metaIDs.get(i + word));
            }
        }

        int matches = 0;

        if( uID != lastUser) {
            userProfile = userProfiles.get(uID);
            userRelDegree = userProfile.size();
            lastUser = uID;
            numUser++;
            if(numUser % 1000 == 0)
                System.out.println(numUser);
        }
        TLongIterator itemMetaWords = itemMetaWordIDs.iterator();
        while(itemMetaWords.hasNext()){
            long word = itemMetaWords.next();
            double userIFIDFForWord = userProfile.get(word);
            if (userIFIDFForWord > 0.0) {
                prediction += userIFIDFForWord;
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
