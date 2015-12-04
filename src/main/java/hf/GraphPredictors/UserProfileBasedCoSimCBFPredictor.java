package hf.GraphPredictors;


import com.google.common.collect.HashBiMap;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.map.hash.TLongDoubleHashMap;
import hf.GraphUtils.*;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.traversal.BranchState;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Uniqueness;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

public class UserProfileBasedCoSimCBFPredictor extends GraphDBPredictor {
    private HashBiMap<String, Long> metaIDs;
    private TIntObjectHashMap<TLongDoubleHashMap> userProfiles;
    private TIntIntHashMap userProfileSupports;
    private Relationships[] relTypes;
    private Labels[] labelTypes;
    private String[] keyValueTypes;
    private HashMap<String, Double> metaWeights;
    private int method;  //prediktalasnal a match-ek szamaval, vagy a userprofil meretevel osztunk?
    private int method2;    //userProfil merete osszes megtalalt szavak szama (1) vagy distinct megtalalt szavak szama (2)


    public void setParameters(GraphDB graphDB, Relationships[] rel_types, HashMap<String, Double> _weights,
                              int method_, int method2_) {
        super.setParameters(graphDB);
        this.relTypes = rel_types;
        this.metaWeights = _weights;
        this.method = method_;
        this.method2 = method2_;

        Labels[] labelTypes = new Labels[rel_types.length];
        String[] keyValueTypes_ = new String[rel_types.length];
        int i = 0;
        for (Relationships rel : rel_types) {
            labelTypes[i] = rel.name().equals(Relationships.ACTS_IN.name()) ? Labels.Actor :
                    (rel.name().equals(Relationships.DIR_BY.name()) ? Labels.Director : Labels.VOD);
            keyValueTypes_[i] = rel.name().equals(Relationships.ACTS_IN.name()) ? GraphDBBuilder.actor :
                    (rel.name().equals(Relationships.DIR_BY.name()) ? GraphDBBuilder.director : GraphDBBuilder.VOD);
            i++;
        }
        this.keyValueTypes = keyValueTypes_;
        this.labelTypes = labelTypes;
    }

    public String getName() {
        return "UserProfile Based CoSim Predictor ";
    }

    public String getShortName(){
        return "CBF_UPB_SIM";
    }

    @Override
    public void printParameters() {
        graphDB.printParameters();
        LogHelper.INSTANCE.logToFile(this.getName() + " Parameters:");
        LogHelper.INSTANCE.logToFile("Súlyok: " + metaWeights.toString());
        LogHelper.INSTANCE.logToFile("Relációk: " + Arrays.toString(relTypes));
        LogHelper.INSTANCE.logToFile("Labelek: " + Arrays.toString(labelTypes));
        LogHelper.INSTANCE.logToFile("Method: " + method);
        LogHelper.INSTANCE.logToFile("Method: " + method2);
    }

    protected void computeSims(boolean uploadResultIntoDB) {
        populateMetaIDs();
        buildUserProfiles();
    }

    public void trainFromGraphDB() {
        graphDB.initDB();
        this.computeSims(false);
    }

    /**
     * String word -> long ID
     */
    private void populateMetaIDs() {
        LogHelper.INSTANCE.logToFileT("MetaID map előállítása:");
        metaIDs = graphDB.getMetaIDs(labelTypes);
        LogHelper.INSTANCE.logToFileT("MetaID map előállítása KÉSZ!");
    }


    private void buildUserProfiles() {
        LogHelper.INSTANCE.logToFileT("Start CoSimCBF with UserProfile:");
        Transaction tx = graphDB.startTransaction();
        ArrayList<Node> userList = graphDB.getNodesByLabel(Labels.User);

//        1 node teszteléséhez:
//        ArrayList<Node> userList = new ArrayList<>();
//        userList.add(graphDB.graphDBService.getNodeById(17020));


        userProfiles = new TIntObjectHashMap<>(userList.size());
        userProfileSupports = new TIntIntHashMap(userList.size());

        TraversalDescription mWordsOfItemsSeenByUser = getNewMetaTraversalByUser(graphDB, relTypes);

        int numOfComputedUserProfiles = 0;
        int changeCounter1 = 0;

        for (Node user : userList) {
            computeProfile(user, mWordsOfItemsSeenByUser);
            changeCounter1++;
            if (changeCounter1 > 30000) {
                graphDB.endTransaction(tx);
                tx = graphDB.startTransaction();
                numOfComputedUserProfiles += changeCounter1;
                changeCounter1 = 0;
                System.out.println(numOfComputedUserProfiles);
            }
        }
        numOfComputedUserProfiles += changeCounter1;

        graphDB.endTransaction(tx);
        LogHelper.INSTANCE.logToFileT("CoSimCBF with UserProfile KÉSZ! " + numOfComputedUserProfiles);
    }

    private void computeProfile(Node user, TraversalDescription description) {
        int userID = (int) user.getProperty(Labels.User.getIDName());
        int userProfileSupp = 0;
        TLongDoubleHashMap mWordFrequencies = new TLongDoubleHashMap();
        for (Path path : description.traverse(user)) {
            String rel = path.lastRelationship().getType().name();
            mWordFrequencies.adjustOrPutValue(path.endNode().getId(), metaWeights.get(rel), metaWeights.get(rel));
            userProfileSupp++;
        }
        userProfiles.put(userID, mWordFrequencies);
        if (method2 == 1)
            userProfileSupports.put(userID, userProfileSupp);
        else
            userProfileSupports.put(userID, mWordFrequencies.size());
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
                .uniqueness(Uniqueness.RELATIONSHIP_PATH);      //eleg az el uniquness, mivel a melyseg alapu relaciotipus
        //lekeres garantalja, hogy ne legyen kor
        //nem unique event list eseten se
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
            HashSet<String> itemWords =
                    GraphDBBuilder.getUniqueItemMetaWordsByKey(this.db, iID, keyValue, graphDB.stopWords);
            for (String word : itemWords) {
                Long metaID = metaIDs.get(i + "" + word);
                if (metaID != null)
                    itemMetaWordIDs.add(metaID);
            }
            i++;
        }

        if (uID != lastUser) {
            userProfile = userProfiles.get(uID);
            userRelDegree = userProfileSupports.get(uID);
            lastUser = uID;
            numUser++;
            if (numUser % 1000 == 0)
                System.out.println(numUser);
        }

        int matches = 0;

        TLongIterator itemMetaWords = itemMetaWordIDs.iterator();
        while (itemMetaWords.hasNext()) {
            long word = itemMetaWords.next();
            double d = userProfile.get(word);
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
