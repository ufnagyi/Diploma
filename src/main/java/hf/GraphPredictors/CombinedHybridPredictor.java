package hf.GraphPredictors;


import gnu.trove.map.hash.TIntDoubleHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import hf.GraphUtils.*;
import onlab.core.Database;

import java.util.HashSet;

public class CombinedHybridPredictor extends GraphDBPredictor {
    private TIntObjectHashMap<TIntDoubleHashMap> itemCFSimilarities;
    private TIntObjectHashMap<TIntDoubleHashMap> itemCBFSimilarities;
    private TIntObjectHashMap<HashSet<Integer>> userItems;
    private int method;
    private int method2;
    private Similarities CFSim;
    private Similarities CBFSim;

    @Override
    protected void computeSims(boolean uploadResultIntoDB) {
    }

    public void setParameters(GraphDB graphDB, Database db, int method, int method2, Similarities cfSim, Similarities cbfSim){
        super.setParameters(graphDB,db);
        this.method = method;
        this.method2 = method2;
        this.CFSim = cfSim;
        this.CBFSim = cbfSim;
    }

    @Override
    public void trainFromGraphDB() {
        graphDB.initDB();
        LogHelper.INSTANCE.log("Adatok betöltése a gráfból:");
        LogHelper.INSTANCE.log("CFSimilarity betöltése a gráfból:");
        itemCFSimilarities = graphDB.getAllSimilaritiesBySim(Labels.Item, CFSim);
        LogHelper.INSTANCE.log("CBFSimilarity betöltése a gráfból:");
        itemCBFSimilarities = graphDB.getAllSimilaritiesBySim(Labels.Item, CBFSim);
        LogHelper.INSTANCE.log("Similarity-k betöltése a gráfból KÉSZ!");
        LogHelper.INSTANCE.log("Felhasználó-item kapcsolatok betöltése a gráfból:");
        userItems = graphDB.getAllUserItems();
        LogHelper.INSTANCE.log("Felhasználó-item kapcsolatok betöltése a gráfból KÉSZ! " + userItems.size() + " user betöltve!" );
        LogHelper.INSTANCE.log("Adatok betöltése a gráfból KÉSZ!");
        graphDB.shutDownDB();
        graphDB = null;
    }


    private int lastUser = -1;
    private int userRelDegree = 0;
    private HashSet<Integer> itemsSeenByUser = new HashSet<>();
    private int numUser = 0;

    @Override
    public double predict(int uID, int iID, long time) {
        int cfMatches =  0;
        int cbfMatches = 0;
        double prediction = 0.0;
        if( uID != lastUser) {
            itemsSeenByUser = userItems.get(uID);
            userRelDegree = itemsSeenByUser.size();
            lastUser = uID;
            numUser++;
            if(numUser % 1000 == 0)
                System.out.println(numUser);
        }

        double cfPrediction = 0.0;
        double cbfPrediction = 0.0;

        for(int i : itemsSeenByUser) {
            double cf;
            double cbf;
            Link<Integer> l = new Link(i,iID);
            TIntDoubleHashMap itemCFSims = itemCFSimilarities.get(l.startNode);
            TIntDoubleHashMap itemCBFSims = itemCBFSimilarities.get(l.startNode);
            cf = itemCFSims == null ? 0.0 : itemCFSims.get(l.endNode);
            cbf = itemCBFSims == null ? 0.0 : itemCBFSims.get(l.endNode);
            if (cf > 0.0) {
                cfPrediction += cf;
                cfMatches++;
            }
            if (cbf > 0.0) {
                cbfPrediction += cbf;
                cbfMatches++;
            }
        }

        if(method == 1) {
            cfPrediction = userRelDegree > 0 ? (cfPrediction / userRelDegree) : 0.0;  //1-es módszer
            cbfPrediction = userRelDegree > 0 ? (cbfPrediction / userRelDegree) : 0.0;
        }
        else {
            cfPrediction = cfMatches > 0 ? cfPrediction / cfMatches : 0.0;         //2-es módszer
            cbfPrediction = cbfMatches > 0 ? cbfPrediction / cbfMatches : 0.0;
        }

        switch(method2) {
            case 1:
                prediction = (cfPrediction + cbfPrediction) / 2;
                break;     //CF és CBF átlaga
        }

        return prediction;
    }
}
