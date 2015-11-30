package hf;

import hf.GraphPredictors.UserProfileBasedCBFPredictor;
import hf.GraphUtils.GraphDB;
import hf.GraphUtils.GraphDBBuilder;
import hf.GraphUtils.Labels;
import hf.GraphUtils.Relationships;
import onlab.core.*;
import onlab.core.RecoPrinter.RecoPrinterBasic;
import onlab.core.evaluation.Evaluation;
import onlab.core.evaluation.SimpleRecallMeasurement;
import onlab.core.predictor.Predictor;
import onlab.core.util.PredictorUtil;
import onlab.core.util.Util;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
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
	public static String getEventsFilename() { return DIR + "events_final.csv"; }
	public static String getDatabaseFullFilename() { return DIR + "db_full.bin"; }
	public static String getDatabaseTrainFilename() { return DIR + "db_train.bin"; }
	public static String getDatabaseTestFilename() { return DIR + "db_test.bin"; }

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

	public static void testPredictors(Predictor[] predictors, Database dbTrain, Database dbTest) throws Exception {
		System.out.println("TESTING PREDICTORS...");

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

	public static void main(String[] args) throws Exception {
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		System.out.println("Futas kezdese: " + dateFormat.format(Calendar.getInstance().getTimeInMillis()));

		//region OldPredictorProcess

//		System.out.println("\nSTEP 1: Importing, building and saving database:");
//		buildDatabaseAndSave();

//		System.out.println("\nSTEP 2: Loading binary databases from file:");
		Database[] dbs = Util.loadDatabases(new String[]{getDatabaseTrainFilename(),getDatabaseTestFilename()}); //getDatabaseTrainFilename(), getDatabaseTestFilename()


//		System.out.println("\nSTEP 3: Predictor training:");
//		Predictor[] preds = trainPredictors(dbs[0]);

		//		System.out.println("\nExample for predictor saving:");
//		Util.savePredictor(preds[0], DIR + "p1.pr");
//		Predictor p1 = Util.loadPredictor(DIR + "p1.pr", dbs[0]);
//		Predictor[] preds = new Predictor[]{p1};

		//endregion


//		------------------------------------------------------------------------
//        //ha a node és relationship csv-ket létre kell hozni az import-toolhoz:
//        Reader r = new Reader(dbs[0]);
//        r.createAllCSVs();
//		-------------------------------------------------------------------------
		//      C:/Nandi_diploma/Neo4J_Database
		//      C:/Nandi_diploma/Test_Database

		GraphDB graphDB = new GraphDB("C:/Nandi_diploma/Neo4J_Database");

//		-------------------------------------------------------------------------
		//GraphDB epites:

		GraphDBBuilder.buildGraphDBFromImpressDB(graphDB,dbs[0],true);

		//TestDB:
//		GraphDB testDB = new GraphDB("C:/Nandi_diploma/Test_Database");
//		GraphDBBuilder.buildGraphDBFromImpressDB(testDB,dbs[1],true);

		//Létrehozott hasonlóság törléséhez:
//		graphDB.deleteSimilaritiesByType(Similarities.CBF_SIM2);

//		-------------------------------------------------------------------------
//		graphDB.initDB();


//		ExampleSimilarityPrinter exampleSimilarityPrinter = new ExampleSimilarityPrinter(graphDB);
//		exampleSimilarityPrinter.printExampleSimilarityResults(10,Similarities.CBF_SIM,Labels.Item);


//		CFGraphPredictor cfGraphPredictor = new CFGraphPredictor();
//		cfGraphPredictor.setParameters(graphDB, dbs[0], 2);
//		cfGraphPredictor.train(true);
//		cfGraphPredictor.trainFromGraphDB();

//		HashMap<String,Double> weights = new HashMap<>(3);
//		weights.put(Relationships.HAS_META.name(),1.0);
//		weights.put(Relationships.ACTS_IN.name(),2.0);
//		weights.put(Relationships.DIR_BY.name(),2.0);
//		Relationships[] relTypes = new Relationships[3];
//		relTypes[0] = Relationships.HAS_META;
//		relTypes[1] = Relationships.ACTS_IN;
//		relTypes[2] = Relationships.DIR_BY;
//		Labels[] labelTypes = new Labels[3];
//		labelTypes[0] = Labels.VOD;
//		labelTypes[1] = Labels.Actor;
//		labelTypes[2] = Labels.Director;
//		String[] keyValueTypes = new String[3];
//		keyValueTypes[0] = "VodMenuDirect";
//		keyValueTypes[1] = "Actor";
//		keyValueTypes[2] = "Director";


//		WordCoSimGraphPredictor wordCoSimGraphPredictor = new WordCoSimGraphPredictor();
//		wordCoSimGraphPredictor.setParameters(graphDB, dbs[0], Similarities.CBF_SIM3, 2, weights, relTypes);
//		wordCoSimGraphPredictor.train(true);
//		wordCoSimGraphPredictor.trainFromGraphDB();

//		UserProfileBasedCBFPredictor userProfileBasedCBFPredictor = new UserProfileBasedCBFPredictor();
//		userProfileBasedCBFPredictor.setParameters(graphDB,dbs[0],2,weights,relTypes,keyValueTypes,labelTypes);
////		userProfileBasedCBFPredictor.train(false);
//		userProfileBasedCBFPredictor.trainFromGraphDB();

//		System.out.println("\nSTEP4: Testing, evaluating predictors and printing random recommendations:");
//		Predictor[] preds = new Predictor[]{wordCoSimGraphPredictor};  //cfGraphPredictor, wordCoSimGraphPredictor, userProfileBasedCBFPredictor


//		testPredictors(preds, dbs[0], dbs[1]);


		System.out.println("Futas vege: " + dateFormat.format(Calendar.getInstance().getTimeInMillis()));
	}
}