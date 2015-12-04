package hf;

import hf.GraphPredictors.*;
import hf.GraphUtils.*;
import onlab.core.*;
import onlab.core.RecoPrinter.RecoPrinterBasic;
import onlab.core.evaluation.Evaluation;
import onlab.core.evaluation.SimpleRecallMeasurement;
import onlab.core.predictor.Predictor;
import onlab.core.util.PredictorUtil;
import onlab.core.util.Util;

import java.io.IOException;
import java.security.InvalidParameterException;
import java.util.Calendar;
import java.util.HashMap;

/**
 * Controller class for Hybrid Filtering project
 * @author David Zibriczky
 */
public class Main {

    public static final String DIR = "data/impresstv_vod_dataset/";

    public static String getItemsFilename() {
        return DIR + "items_final.csv";
    }

    public static String getEventsFilename() {
        return DIR + "events_final.csv";
    }

    public static String getDatabaseFullFilename() {
        return DIR + "db_full.bin";
    }

    public static String getDatabaseTrainFilename() {
        return DIR + "db_train.bin";
    }

    public static String getDatabaseTestFilename() {
        return DIR + "db_test.bin";
    }

    public final static String rootDBDir = "C:/Nandi_diploma/";
    public final static String dbNoFilterAllEvents = rootDBDir + "Neo4J_NoFilter_AllEvents";
    public final static String dbFilterAllEvents = rootDBDir + "Neo4J_Filter_AllEvents";
    public final static String dbNoFilterUniqueEvents = rootDBDir + "Neo4J_NoFilter_UniqueEvents";
    public final static String dbFilterUniqueEvents = rootDBDir + "Neo4J_Filter_UniqueEvents";
    public static Database dbTrain;
    public static Database dbTest;


    public static void buildDatabaseAndSave() throws Exception {

        System.out.println("Setting file paths...");
        HashMap<String, String> path = new HashMap<>();
        path.put(ExtendedDatabase.ITEM, getItemsFilename());
        path.put(ExtendedDatabase.EVENT, getEventsFilename());

        System.out.println("Setting item metadata list to load...");
        ExtendedDatabase db = new ExtendedDatabase();
        db.setParameter(ExtendedDatabase.ADDITIONAL_ITEM_COLUMNS,
                "ItemId:string"
                        + ",Hidden:string"
                        + ",TimeStamp:string"
                        + ",Actor:string"
                        + ",AllPerson:string"
                        + ",AvailabilityWindowEnd:string"
                        + ",AvailabilityWindowStart:string"
                        + ",Description:string"
                        + ",Director:string"
                        + ",DuplicateFilterValue:string"
                        + ",DurationSeconds:string"
                        + ",HasMdsId:string"
                        + ",ImageHashNew:string"
                        + ",ImageHashNormal:string"
                        + ",IsAdult:string"
                        + ",IsComingSoon:string"
                        + ",IsHD:string"
                        + ",IsLastChance:string"
                        + ",IsNewRelease:string"
                        + ",ItemType:string"
                        + ",LastChanceWindowBegin:string"
                        + ",LicensingWindowEnd:string"
                        + ",LicensingWindowStart:string"
                        + ",NewReleaseWindowEnd:string"
                        + ",Producer:string"
                        + ",ProviderId:string"
                        + ",ProviderResourceId:string"
                        + ",ReleaseYear:string"
                        + ",ResourceId:string"
                        + ",SuggestedPrice:string"
                        + ",Title:string"
                        + ",VisibleAndPurchaseWindowEnd:string"
                        + ",VisibleWindowStart:string"
                        + ",VodMenuAll:string"
                        + ",VodMenuDirect:string"
        );

        System.out.println("Setting event metadata list to load...");
        db.setParameter(ExtendedDatabase.ADDITIONAL_EVENT_COLUMNS, "Price:float");

        System.out.println("Setting other dataset constants for importer...");
        db.EVENT_TIME_STR = "Time";
        db.EVENT_TYPE_STR = "EventType";
        db.ITEM_ID_STR_IN_EVENTS = "ItemId";
        db.USER_ID_STR = "UserId";
        db.ITEM_ID_STR = "ItemId";
        db.EVENT_TIME_IN_SECONDS = true;

//		db.ITEM_FROM_DATE_STR = "AvailabilityWindowStart"; // -1, amennyiben ismeretlen
//		db.ITEM_TO_DATE_STR = "AvailabilityWindowEnd"; // -1, amennyiben ismeretlen

        System.out.println("Loading databases from file...");
        db.loadFromTxt(path);

        System.out.println("Statistics about full database:");
        new DatabaseStats(db).printSummary();
        System.out.println("Saving full database");
        Util.saveObject(db, getDatabaseFullFilename());

        System.out.println("Splitting database...");
        Database db2 = db.splitByTime("2013-11-15 00:00:00", "2:5000", "2:14");
        System.out.println("Statistics about train database:");
        new DatabaseStats(db).printSummary();
        System.out.println("Statistics about test database:");
        new DatabaseStats(db2).printSummary();

        System.out.println("Saving train database...");
        Util.saveObject(db, getDatabaseTrainFilename());
        System.out.println("Saving test database...");
        Util.saveObject(db2, getDatabaseTestFilename());
    }

    public static Predictor[] trainPredictors(Database dbTrain) throws Exception {
        System.out.println("TRAINING PREDICTORS...");

//        System.out.println("Training WordBased Predictor ...");
//        Predictor pWordBased = new WordBasedPredictor();
//        pWordBased.setParameters(ExamplePredictor.META_KEYS + "=VodMenuDirect:0.5 Actor:0.8 Director:0.5");
//        pWordBased.train(dbTrain, null);
//        System.out.println("");

//        System.out.println("Training ContentBased Predictor ...");
//        Predictor pContentBased = new ContentBasedPredictor();
//        pContentBased.setParameters(ExamplePredictor.META_KEYS + "=VodMenuDirect:0.5 Actor:0.8 Director:0.5");
//        pContentBased.train(dbTrain, null);
//        System.out.println("");

//        System.out.println("Training ItemBasedCollabor Predictor ...");
//        Predictor pItemBasedCollab = new ItemBasedCollaborPredictor();
//        pItemBasedCollab.setParameters(ExamplePredictor.META_KEYS + "=LAMBDA:10");
//        pItemBasedCollab.train(dbTrain, null);
//        System.out.println("");

        //region Default predictors

//		System.out.println("Training Pop Predictor 1...");
//		Predictor pPop1 = new PopPredictor();
//		pPop1.setParameters(PopPredictor.E_WINDOW + "=*:180");
//		pPop1.train(dbTrain, null);
//		System.out.println("");
//
//		System.out.println("Training Pop Predictor 2...");
//		Predictor pPop2 = new PopPredictor();
//		pPop2.setParameters(PopPredictor.E_WINDOW + "=*:15");
//		pPop2.train(dbTrain, null);
//		System.out.println("");
//
//		System.out.println("Training IALS Predictor...");
//		Predictor pIALS = new IALSPredictor();
//		pIALS.setParameters(IALSPredictor.N_FACTORS + "=20");
//		pIALS.train(dbTrain, null);
//		System.out.println("");
//
//		System.out.println("Training CoSim Predictor...");
//		Predictor pCoSim = new CoSimPredictor();
//		pCoSim.setParameters(CoSimPredictor.N_USER_EVENTS + "=10");
//		pCoSim.train(dbTrain, null);
//		System.out.println("");
//
//		System.out.println("Training Metadata Cosine Similarity Predictor 1...");
//		Predictor pCosine1 = new FastCosineSimilarityPredictor();
//		pCosine1.setParameters(FastCosineSimilarityPredictor.META_KEYS + "=VodMenuDirect:1");
//		pCosine1.train(dbTrain, null);
//		System.out.println("");
//
//		System.out.println("Training Metadata Cosine Similarity Predictor 2...");
//		Predictor pCosine2 = new FastCosineSimilarityPredictor();
//		pCosine2.setParameters(FastCosineSimilarityPredictor.META_KEYS + "=VodMenuDirect:1 Actor:0.2 IsAdult:0.1");
//		pCosine2.train(dbTrain, null);
//		System.out.println("");

        //endregion

        System.out.println("Returning the list of predictors.");
        return new Predictor[]{
//		pWordBased, pItemBasedCollab, pContentBased,  , pExample, pPop1, pPop2, pIALS, pCoSim, pCosine1, pCosine2
        };
    }

    public static void testPredictorsOLD(Predictor[] predictors, Database dbTrain, Database dbTest) throws Exception {
        LogHelper.INSTANCE.logToFileT("TESTING PREDICTORS...");

        System.out.println("Initializing Recall measurement class");
        Evaluation eval = new SimpleRecallMeasurement();
        eval.setParameters(SimpleRecallMeasurement.RETURN_RECALL_IN + "=10");
        eval.setParameters(SimpleRecallMeasurement.EVENT_SAMPLE_RATIO + "=0.1");
        eval.prepare(dbTest, dbTrain);
        System.out.println("Evaluation parameters: " + eval.getParameters().toString(","));

        System.out.println("Calculating the max time of training dataset for random recommendations...");
        long time = dbTrain.getMaxTimeByEvents();

        System.out.println("Initializing Recommendations Printer class...");
        RecoPrinter rp = new RecoPrinterBasic();
        rp.setParameters(RecoPrinterBasic.ITEM_META_KEYS + "=ItemId:VodMenuDirect");

        //RecoPrinter rp = new RecoPrinterHTML();
        //rp.setParameters("nUserEvents=20,itemMetaKeys=ItemId:VodMenuDirect:Actor,imagePrefix=,imagePostfix=,imageMetaKey=,titleMetaKey=Title");

        System.out.println("Selecting random users to sample personalized recommendations...");
        int[] userIdxsToPrint = PredictorUtil.selectRandomUsers(dbTrain, 42, 15, 2, 20);
        System.out.println("Selecting random items to sample item2item lists...");
        int[] itemIdxsToPrint = PredictorUtil.selectRandomItems(dbTrain, 42, 15, 0, 1000);

        System.out.println("Testing predictors...");
        for (int i = 0; i < predictors.length; i++) {
            Predictor pr = predictors[i];
            System.out.println("Predictor to evaluate: " + pr.getClass().getSimpleName());
            System.out.println("Parameters: " + pr.getParameters().toString(","));

            double val = eval.run(pr);
            System.out.printf("%s: %.6f\n", eval.getClass().getSimpleName(), val);

            System.out.println("Printing recommendations to file...");
            ItemRecommendation[] iRecs = Recommender.getItemRecommendationBulk(pr, userIdxsToPrint, time, 10, null);
            rp.printItemRecs(iRecs, dbTrain, DIR + "iRec_" + System.currentTimeMillis() + "_p" + i + ".txt");

//			System.out.println("Printing item2item lists to file...");
//			Item2ItemList[] i2is = Recommender.getItem2ItemListBulk(pr, itemIdxsToPrint, time, 10, null);
//			rp.printItem2Items(i2is, dbTrain, DIR + "i2i_" + System.currentTimeMillis() + "_p" + i + ".txt");
//			System.out.println("");
        }
    }

    public static void testPredictor(GraphDBPredictor predictor) throws Exception {
        LogHelper.INSTANCE.logSeparatorToFile();
        LogHelper.INSTANCE.logToFileT("TESTING GRAPH PREDICTOR " + predictor.getName());

        System.out.println("Initializing Recall measurement class");
        Evaluation eval = new SimpleRecallMeasurement();
        eval.setParameters(SimpleRecallMeasurement.RETURN_RECALL_IN + "=10");
        eval.setParameters(SimpleRecallMeasurement.EVENT_SAMPLE_RATIO + "=0.1");
        eval.prepare(dbTest, dbTrain);
        System.out.println("Evaluation parameters: " + eval.getParameters().toString(","));

        System.out.println("Initializing Recommendations Printer class...");
        RecoPrinter rp = new RecoPrinterBasic();
        rp.setParameters(RecoPrinterBasic.ITEM_META_KEYS + "=ItemId:VodMenuDirect");

        System.out.println("Calculating the max time of training dataset for random recommendations...");
        long time = dbTrain.getMaxTimeByEvents();

        System.out.println("Selecting random users to sample personalized recommendations...");
        int[] userIdxsToPrint = PredictorUtil.selectRandomUsers(dbTrain, 42, 15, 2, 20);
        System.out.println("Selecting random items to sample item2item lists...");
        int[] itemIdxsToPrint = PredictorUtil.selectRandomItems(dbTrain, 42, 15, 0, 1000);


        double val = eval.run(predictor);
        LogHelper.INSTANCE.logToFile(eval.getClass().getSimpleName() + ": " + val);
        Calendar cal = Calendar.getInstance();
        String recommenderListFileName = predictor.getShortName() + "_"
                + cal.get(Calendar.MONTH) + "_" + cal.get(Calendar.DAY_OF_MONTH) + cal.getTimeInMillis() + ".txt";
        LogHelper.INSTANCE.logToFile("Recommender list file: " + recommenderListFileName);

        System.out.printf("%s: %.6f\n", eval.getClass().getSimpleName(), val);
        System.out.println("Printing recommendations to file...");
        ItemRecommendation[] iRecs = Recommender.getItemRecommendationBulk(predictor, userIdxsToPrint, time, 10, null);
        rp.printItemRecs(iRecs, dbTrain, DIR + recommenderListFileName);
        LogHelper.INSTANCE.printMemUsage();
        LogHelper.INSTANCE.logToFileT("TESTING GRAPH PREDICTOR " + predictor.getName() + " DONE!");
    }

    public static void main(String[] args) throws Exception {
        LogHelper.INSTANCE.logToFileT("Futas kezdése:");
        //getDatabaseTrainFilename(), getDatabaseTestFilename()
        Database[] dbs = Util.loadDatabases(new String[]{getDatabaseTrainFilename(), getDatabaseTestFilename()});
        dbTrain = dbs[0];
        dbTest = dbs[1];
        LogHelper.INSTANCE.printMemUsage();
//		-------------------------------------------------------------------------
        GraphDB graphDB = buildGraphDB(dbFilterAllEvents);
        graphDB = null;
        System.gc();
        graphDB = buildGraphDB(dbFilterUniqueEvents);
        graphDB = null;
        System.gc();
        graphDB = buildGraphDB(dbNoFilterAllEvents);
        graphDB = null;
        System.gc();
        graphDB = buildGraphDB(dbNoFilterUniqueEvents);

//		-------------------------------------------------------------------------

//        GraphDB graphDB = new GraphDB(dbTrain, dbFilterAllEvents, false, true);
//        testCFPredictor(graphDB, Similarities.CF_ISIM, 1, 0);
//        testCFPredictor(graphDB, Similarities.CF_ISIM2, 1, 1);

//        ExampleSimilarityPrinter exampleSimilarityPrinter = new ExampleSimilarityPrinter(graphDB);
//		exampleSimilarityPrinter.printExampleSimilarityResults(10,Similarities.CBF_SIM,Labels.Item);

        HashMap<String, Double> weights = new HashMap<>(3);
        weights.put(Relationships.HAS_META.name(), 1.0);
        weights.put(Relationships.ACTS_IN.name(), 2.0);
        weights.put(Relationships.DIR_BY.name(), 2.0);
        Relationships[] relTypes = new Relationships[3];
        relTypes[0] = Relationships.HAS_META;
        relTypes[1] = Relationships.ACTS_IN;
        relTypes[2] = Relationships.DIR_BY;


//        WordBasedCoSimCBFGraphPredictor wordBasedCoSimCBFGraphPredictor = new WordBasedCoSimCBFGraphPredictor();
//        wordBasedCoSimCBFGraphPredictor.setParameters(graphDB, dbTrain, Similarities.CBF_SIM, 1, weights, relTypes, 4.0);
//		wordBasedCoSimCBFGraphPredictor.train(true);
//        wordBasedCoSimCBFGraphPredictor.trainFromGraphDB();
//
//		UserProfileBasedTFiDF_CBFPredictor userProfileBasedTFiDF_CBFPredictor = new UserProfileBasedTFiDF_CBFPredictor();
//		userProfileBasedTFiDF_CBFPredictor.setParameters(graphDB,dbTrain,1,relTypes,keyValueTypes,labelTypes, stopWordsFileName, uniqueEvents);
//		userProfileBasedTFiDF_CBFPredictor.train(true);
//		userProfileBasedTFiDF_CBFPredictor.trainFromGraphDB();

//        UserProfileBasedCoSimCBFPredictor userProfileBasedCoSimCBFPredictor = new UserProfileBasedCoSimCBFPredictor();
//        userProfileBasedCoSimCBFPredictor.setParameters(graphDB,dbTrain,relTypes,labelTypes,keyValueTypes, stopWordsFileName, weights,2,1, uniqueEvents);
//        userProfileBasedCoSimCBFPredictor.train(false);

//        CombinedHybridPredictor combinedHybridPredictor = new CombinedHybridPredictor();
//        combinedHybridPredictor.setParameters(graphDB,dbTrain,2,1,Similarities.CF_ISIM, Similarities.CBF_SIM);
//        combinedHybridPredictor.trainFromGraphDB();

//        HybridComputedPredictor hybridComputedPredictor = new HybridComputedPredictor();
//        hybridComputedPredictor.setParameters(graphDB,dbTrain,Similarities.HF_SIM,2,uniqueEvents,weights,relTypes);
//        hybridComputedPredictor.train(true);
//        hybridComputedPredictor.trainFromGraphDB();

        LogHelper.INSTANCE.logToFileT("Futas vége:");
        LogHelper.INSTANCE.logToFile("separator");
        LogHelper.INSTANCE.close();
    }

    public static void testCFPredictor(GraphDB graphDB, Similarities sim, int method, int minSuppAB) throws Exception {
        CFGraphPredictor cfGraphPredictor = new CFGraphPredictor();
        cfGraphPredictor.setParameters(graphDB, sim, method, minSuppAB);
        cfGraphPredictor.train(true);
        System.gc();
        cfGraphPredictor.trainFromGraphDB();
        testPredictor(cfGraphPredictor);
        LogHelper.INSTANCE.printMemUsage();
        System.gc();
    }

    public static GraphDB buildGraphDB(String dbType) throws IOException {
        switch (dbType) {
            case dbNoFilterAllEvents:
                LogHelper.INSTANCE.logToFile("No Filter + All Events DB");
                GraphDB NoFilterAllEventsDB = new GraphDB(dbTrain, dbNoFilterAllEvents, false, false);
                GraphDBBuilder.buildGraphDBFromImpressDB(NoFilterAllEventsDB, true);
                LogHelper.INSTANCE.printMemUsage();
                LogHelper.INSTANCE.logSeparatorToFile();
                return NoFilterAllEventsDB;
            case dbFilterAllEvents:
                LogHelper.INSTANCE.logToFile("Filter + All Events DB");
                GraphDB FilterAllEventsDB = new GraphDB(dbTrain, dbFilterAllEvents, false, true);
                GraphDBBuilder.buildGraphDBFromImpressDB(FilterAllEventsDB, true);
                LogHelper.INSTANCE.printMemUsage();
                LogHelper.INSTANCE.logSeparatorToFile();
                return FilterAllEventsDB;
            case dbNoFilterUniqueEvents:
                LogHelper.INSTANCE.logToFile("No Filter + Unique Events DB");
                GraphDB NoFilterUniqueEventsDB = new GraphDB(dbTrain, dbNoFilterUniqueEvents, true, false);
                GraphDBBuilder.buildGraphDBFromImpressDB(NoFilterUniqueEventsDB, true);
                LogHelper.INSTANCE.printMemUsage();
                LogHelper.INSTANCE.logSeparatorToFile();
                return NoFilterUniqueEventsDB;
            case dbFilterUniqueEvents:
                LogHelper.INSTANCE.logToFile("Filter + Unique Events DB");
                GraphDB FilterUniqueEventsDB = new GraphDB(dbTrain, dbFilterUniqueEvents, true, true);
                GraphDBBuilder.buildGraphDBFromImpressDB(FilterUniqueEventsDB, true);
                LogHelper.INSTANCE.printMemUsage();
                LogHelper.INSTANCE.logSeparatorToFile();
                return FilterUniqueEventsDB;
            default:
                throw new InvalidParameterException("Nincs ilyen gráfDB típus!");
        }
    }
}