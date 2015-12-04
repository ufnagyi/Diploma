package hf.GraphPredictors;


import com.google.common.collect.HashBiMap;
import gnu.trove.iterator.TLongIntIterator;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.map.hash.TLongDoubleHashMap;
import gnu.trove.map.hash.TLongIntHashMap;
import hf.GraphUtils.*;
import onlab.core.Database;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.traversal.TraversalDescription;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;

public class UserProfileBasedTFiDF_CBFPredictor extends GraphDBPredictor {
    private HashBiMap<String, Long> metaIDs;
    private TIntObjectHashMap<TLongDoubleHashMap> userProfiles;
    private Relationships[] relTypes;
    private Labels[] labelTypes;
    private String[] keyValueTypes;
    private TLongDoubleHashMap wordIDFs;
    private int method;


    public void setParameters(GraphDB graphDB, int method,
                              Relationships[] rel_types) {
        super.setParameters(graphDB);
        this.method = method;
        this.relTypes = rel_types;

        Labels[] labelTypes = new Labels[rel_types.length];
        String[] keyValueTypes_ = new String[rel_types.length];
        int i = 0;
        for (Relationships rel : rel_types) {
            labelTypes[i] = rel.name().equals(Relationships.ACTS_IN.name()) ? Labels.Actor :
                    (rel.name().equals(Relationships.DIR_BY.name()) ? Labels.Director : Labels.VOD);
            keyValueTypes_[i] = rel.name().equals(Relationships.ACTS_IN.name()) ? "Actor" :
                    (rel.name().equals(Relationships.DIR_BY.name()) ? "Director" : "VodMenuDirect");
            i++;
        }
        this.keyValueTypes = keyValueTypes_;
        this.labelTypes = labelTypes;
    }

    public String getName() {
        return "User Profile Based TF-iDF Predictor ";
    }

    public String getShortName(){
        return "CBF_TFIDF";
    }

    @Override
    public void printParameters() {
        graphDB.printParameters();
        LogHelper.INSTANCE.logToFile(this.getName() + " Parameters:");
        LogHelper.INSTANCE.logToFile("Relációk: " + Arrays.toString(relTypes));
        LogHelper.INSTANCE.logToFile("Labelek: " + Arrays.toString(labelTypes));
        LogHelper.INSTANCE.logToFile("Method: " + method);
    }

    protected void computeSims(boolean uploadResultIntoDB) {
        populateMetaIDs();
        buildUserProfiles();
    }

    public void trainFromGraphDB() {
        graphDB.initDB();
        computeSims(false);
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
        LogHelper.INSTANCE.logToFileT("Start CBFSim with UserProfile and TFiDF:");
        Transaction tx = graphDB.startTransaction();
        ArrayList<Node> userList = graphDB.getNodesByLabel(Labels.User);

//        1 node teszteléséhez:
//        ArrayList<Node> userList = new ArrayList<>();
//        userList.add(graphDB.graphDBService.getNodeById(17020));


        // word_IDF = numOfItems / numOfItemsContainingTheWord
        wordIDFs = graphDB.getMetaIDFValues(labelTypes);

        userProfiles = new TIntObjectHashMap<>(userList.size());

        TraversalDescription mWordsOfItemsSeenByUser =
                UserProfileBasedCoSimCBFPredictor.getNewMetaTraversalByUser(graphDB, relTypes);

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
        LogHelper.INSTANCE.logToFileT("CBFSim with UserProfile and TFiDF KÉSZ! " + numOfComputedUserProfiles);
    }

    private void computeProfile(Node user, TraversalDescription description) {

        int userID = (int) user.getProperty(Labels.User.getIDName());
        TLongIntHashMap mWordFrequencies = new TLongIntHashMap();
        int numOfMetaWords = 0;

        for (Path path : description.traverse(user)) {
            Node mWord = path.endNode();
            mWordFrequencies.adjustOrPutValue(mWord.getId(), 1, 1);
            numOfMetaWords++;
//            System.out.println(mWord.getAllProperties());
        }
        TLongDoubleHashMap userProfile = new TLongDoubleHashMap(mWordFrequencies.size());
        TLongIntIterator mWordFrequency = mWordFrequencies.iterator();
        while (mWordFrequency.hasNext()) {
            mWordFrequency.advance();
            long metaWord = mWordFrequency.key();
            int wordFrequency = mWordFrequency.value();
//            System.out.println(metaIDs.inverse().get(metaWord) + ": " + wordFrequency);
            double relativeWordFrequency = (double) wordFrequency / numOfMetaWords;
            double tf_iDF = relativeWordFrequency * wordIDFs.get(metaWord);
            userProfile.put(metaWord, tf_iDF);
        }
        userProfiles.put(userID, userProfile);
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

        int matches = 0;

        if (uID != lastUser) {
            userProfile = userProfiles.get(uID);
            userRelDegree = userProfile.size();
            lastUser = uID;
            numUser++;
            if (numUser % 1000 == 0)
                System.out.println(numUser);
        }
        TLongIterator itemMetaWords = itemMetaWordIDs.iterator();
        while (itemMetaWords.hasNext()) {
            long word = itemMetaWords.next();
            double userIFIDFForWord = userProfile.get(word);
            if (userIFIDFForWord > 0.0) {
                prediction += userIFIDFForWord;
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
