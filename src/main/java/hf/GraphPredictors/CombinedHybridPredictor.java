package hf.GraphPredictors;


import gnu.trove.map.hash.TIntDoubleHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import hf.GraphUtils.*;

import java.util.HashSet;

public class CombinedHybridPredictor extends GraphDBPredictor {
    private TIntObjectHashMap<TIntDoubleHashMap> itemCFSimilarities;
    private TIntObjectHashMap<TIntDoubleHashMap> itemCBFSimilarities;
    private TIntObjectHashMap<HashSet<Integer>> userItems;
    private int method;     //hogyan atlagoljam az adott recommender alg-ot
    private double CF_SIM_WEIGHT;    //hogyan atlagoljam a ket alg eredmenyet egybe
    private Similarities CFSim;
    private Similarities CBFSim;
    private double CBF_SIM_WEIGHT;

    @Override
    protected void computeSims(boolean uploadResultIntoDB) {
        trainFromGraphDB();
    }

    public String getName(){
        return "Combined Hybrid Predictor ";
    }

    public String getShortName(){
        return "HYB_COMBINED";
    }

    public void setParameters(GraphDB graphDB, int method, double cf_weight, Similarities cfSim, Similarities cbfSim) {
        super.setParameters(graphDB);
        this.method = method;
        this.CF_SIM_WEIGHT = cf_weight;
        this.CBF_SIM_WEIGHT = 1.0 - CF_SIM_WEIGHT;
        this.CFSim = cfSim;
        this.CBFSim = cbfSim;
        this.numUser = 0;
    }

    @Override
    public void printParameters() {
        graphDB.printParameters();
        LogHelper.INSTANCE.logToFile(this.getName() + " Parameters:");
        LogHelper.INSTANCE.logToFile("Method: " + method);
        LogHelper.INSTANCE.logToFile("CF_SIM_WEIGHT: " + CF_SIM_WEIGHT);
        LogHelper.INSTANCE.logToFile("CF_SIM: " + CFSim.name());
        LogHelper.INSTANCE.logToFile("CBF_SIM: " + CBFSim.name());
    }

    @Override
    public void trainFromGraphDB() {
        graphDB.initDB();
        printParameters();
        LogHelper.INSTANCE.logToFileStartTimer("Adatok betöltése a gráfból:");
        LogHelper.INSTANCE.logToFileT("CFSimilarity betöltése a gráfból:");
        itemCFSimilarities = graphDB.getAllSimilaritiesBySim(Labels.Item, CFSim);
        LogHelper.INSTANCE.logToFileT("CBFSimilarity betöltése a gráfból:");
        itemCBFSimilarities = graphDB.getAllSimilaritiesBySim(Labels.Item, CBFSim);
        LogHelper.INSTANCE.logToFileT("Similarity-k betöltése a gráfból KÉSZ!");
        LogHelper.INSTANCE.logToFileT("Felhasználó-item kapcsolatok betöltése a gráfból:");
        userItems = graphDB.getAllUserItems();
        LogHelper.INSTANCE.logToFileT("Felhasználó-item kapcsolatok betöltése a gráfból KÉSZ! " + userItems.size() + " user betöltve!");
        LogHelper.INSTANCE.logToFileT("Adatok betöltése a gráfból KÉSZ!");
        LogHelper.INSTANCE.logToFileStopTimer("Runtime:");
        graphDB.shutDownDB();
        LogHelper.INSTANCE.printMemUsage();
    }


    private int lastUser = -1;
    private int userRelDegree = 0;
    private HashSet<Integer> itemsSeenByUser = new HashSet<>();
    private int numUser = 0;

    @Override
    public double predict(int uID, int iID, long time) {
        int cfMatches = 0;
        int cbfMatches = 0;

        if (uID != lastUser) {
            itemsSeenByUser = userItems.get(uID);
            userRelDegree = itemsSeenByUser.size();
            lastUser = uID;
            numUser++;
            if (numUser % 1000 == 0)
                System.out.println(numUser);
        }

        double cfPrediction = 0.0;
        double cbfPrediction = 0.0;

        for (int i : itemsSeenByUser) {
            double cf;
            double cbf;
            Link<Integer> l = new Link<>(i, iID);
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

        if (method == 1) {
            cfPrediction = userRelDegree > 0 ? (cfPrediction / userRelDegree) : 0.0;  //1-es módszer
            cbfPrediction = userRelDegree > 0 ? (cbfPrediction / userRelDegree) : 0.0;
        } else if (method == 2) {
            cfPrediction = cfMatches > 0 ? cfPrediction / cfMatches : 0.0;         //2-es módszer
            cbfPrediction = cbfMatches > 0 ? cbfPrediction / cbfMatches : 0.0;
        } else if (method == 3) {
            cfPrediction = userRelDegree > 0 ? (cfPrediction / userRelDegree) : 0.0;  //1-es módszer
            cbfPrediction = cbfMatches > 0 ? cbfPrediction / cbfMatches : 0.0;      //2-es módszer
        } else {
            cfPrediction = cfMatches > 0 ? cfPrediction / cfMatches : 0.0;          //2-es módszer
            cbfPrediction = userRelDegree > 0 ? (cbfPrediction / userRelDegree) : 0.0;      //1-es módszer
        }

        return (CF_SIM_WEIGHT * cfPrediction + CBF_SIM_WEIGHT * cbfPrediction) / 2;
    }
}
