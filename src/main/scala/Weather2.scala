//import org.apache.spark._
//import org.apache.spark.ml.classification.{LogisticRegression, RandomForestClassifier}
//import org.apache.spark.ml.feature.{Imputer, StringIndexer, VectorAssembler}
//import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.{DataFrame, SQLContext, SQLImplicits, SparkSession}
//import org.apache.spark.sql.DataFrame
//import org.apache.spark.sql.types.{DoubleType, FloatType, IntegerType, StringType, StructField, StructType}
//import shapeless.syntax.std.tuple.productTupleOps
//import org.apache.log4j.{Level, Logger}
//import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
//
//import java.io.FileWriter
//
//
//
////      WeatherCSVMapping(fields2(0),
////                      fields2(1),
////                      fields2(2),
////                      fields2(3),
////                      fields2(4),
////                      fields2(5),
////                      fields2(6),
////                      fields2(7),
////                      fields2(8),
////                      fields2(9),
////                      fields2(10),
////                      fields2(11),
////                      fields2(12),
////                      fields2(13),
////                      fields2(14),
////                      fields2(15),
////                      fields2(16),
////                      fields2(17),
////                      fields2(18),
////                      fields2(19),
////                      fields2(20),
////                      fields2(21),
////                      fields2(22),
////                      fields2(23),
////                      fields2(24),
////                      fields2(25),
////                      fields2(26),
////                      fields2(27),
////                      fields2(28),
////                      fields2(29),
////                      fields2(30),
////                      fields2(31))
////    })
//
//case class WeatherCSVMapping(station: String,
//                         valid: String,
//                         longitude: String,
//                         latitude: String,
//                         elevation: String,
//                         tmpf: String,
//                         dwpf: String,
//                         relh: String,
//                         drct: String,
//                         sknt: String,
//                         p01i: String,
//                         alti: String,
//                         mslp: String,
//                         vsby: String,
//                         gust: String,
//                         skyc1: String,
//                         skyc2: String,
//                         skyc3: String,
//                         skyc4: String,
//                         skyl1: String,
//                         skyl2: String,
//                         skyl3: String,
//                         skyl4: String,
//                         wxcodes: String,
//                         ice_accretion_1hr: String,
//                         ice_accretion_3hr: String,
//                         ice_accretion_6hr: String,
//                         peak_wind_gust: String,
//                         peak_wind_drct: String,
//                         peak_wind_time: String,
//                         feel: String,
//                         metar: String)
//
//case class WeatherModifiedCSVMapping(
//                             longitude: String,
//                             latitude: String,
//                             elevation: String,
//                             tmpf: String,
//                             dwpf: String,
//                             relh: String,
//                             drct: String,
//                             sknt: String,
//                             p01i: String,
//                             alti: String,
//                             mslp: String,
//                             vsby: String,
//                             gust: String,
//                             skyl1: String,
//                             skyl2: String,
//                             skyl3: String,
//                             skyl4: String,
//                             wxcodes: String,
//                             ice_accretion_1hr: String,
//                             ice_accretion_3hr: String,
//                             ice_accretion_6hr: String,
//                             peak_wind_gust: String,
//                             peak_wind_drct: String,
//                             feel: String)
//
//
//object Weather {
//
//  def RandomForest(forecastDf: DataFrame): Unit ={
////    val imputer = new Imputer()
////      .setInputCols(forecastDf.columns)
////      .setOutputCols(forecastDf.columns.map(c => s"${c}_imputed"))
////      .setStrategy("median")
////
////    //
////    imputer.fit(forecastDf).transform(forecastDf).show()
//
//    //forecastDf.show()
//
//
//    // Create Vectors
//    val cols = Array("longitude",
//      "latitude",
//      "elevation",
//      "tmpf",
//      "dwpf",
//      "relh",
//      "drct",
//      "sknt",
//      "p01i",
//      "alti",
//      "mslp",
//      "vsby",
//      "gust",
//      "skyl1",
//      "skyl2",
//      "skyl3",
//      "skyl4",
//      "ice_accretion_1hr",
//      "ice_accretion_3hr",
//      "ice_accretion_6hr",
//      "peak_wind_gust",
//      "peak_wind_drct",
//    "feel")
//
//    val assembler = new VectorAssembler()
//      .setInputCols(cols)
//      .setOutputCol("features")
//    val featureDf = assembler.transform(forecastDf)
//    //featureDf.printSchema()
//
//
//    val indexer = new StringIndexer()
//      .setInputCol("wxcodes")
//      .setOutputCol("label")
//    val labelDf = indexer.fit(featureDf).transform(featureDf)
//    //labelDf.printSchema()
//
////    val lr = new LogisticRegression()
////      .setMaxIter(10)
////      .setRegParam(0.3)
////      .setElasticNetParam(0.8)
////    val lrmodel = lr.fit(featureDf)
////    val trainingSummary = lrmodel.summary
////    println(trainingSummary)
//
//  //  Random Forest
//    val seed = 5043
//
//    val Array(trainingData, testData) = labelDf.randomSplit(Array(0.7,0.3), seed)
//
//    val randomForestClassifier = new RandomForestClassifier()
//      .setImpurity("gini")
//      .setMaxDepth(30)
//      .setMaxBins(42)
//      .setNumTrees(40)
//      .setFeatureSubsetStrategy("auto")
//      .setSeed(seed)
//
//    val randomForestModel = randomForestClassifier.fit(trainingData)
//    println(randomForestModel.toDebugString)
//
//
//    val predictionDf = randomForestModel.transform(testData)
//    predictionDf.select("prediction", "label", "features").show(1500)
//
//    val eval = new MulticlassClassificationEvaluator()
//      .setLabelCol("label")
//      .setPredictionCol("prediction")
//      .setMetricName("accuracy")
//    val acc = eval.evaluate(predictionDf)
//    val error = 1.0-acc
//    println(s"Test Error = $error")
//  }
//
//  def codeWX(wxcode: String): String ={
//    var newCode = "0"
//    if (wxcode == "RA"){
//      newCode = "1"
//    }
//    if (wxcode == "SN"){
//      newCode = "2"
//    }
//    if (wxcode == "FZRA"){
//      newCode = "3"
//    }
//   newCode
//  }
//
//  def ReplaceMissing(field: String): String ={
//    if (field == ("M") || field == ("T")){
//      return "0"
//    }
//    field
//  }
//
//  def main(args: Array[String]): Unit ={
//    val sparkConf = new SparkConf().setAppName("Asos").setMaster("local")
//    val sc = new SparkContext(sparkConf)
//    val spark = SparkSession
//      .builder()
//      .appName("Asos2")
//      .config("master", "local[*]")
//      .getOrCreate()
//    val SQLContext = new SQLContext(sc)
//    import SQLContext.implicits._
//
//    Logger.getLogger("org").setLevel(Level.ERROR)
//
////    val rddFromFile = sc.textFile("asos.csv")
////
////    // Filter on WXCodes we want
////    val forecast = rddFromFile.filter(row => {
////      val fields = row.split(",").map(_.trim)
////      (fields(2) != ("M") &&
////        fields(3) != ("M") &&
////        fields(4) != ("M"))
////    }).map(row => {
////      println(row)
////      val fields2 = row.split(",").map(_.trim)
////      WeatherModifiedCSVMapping(ReplaceMissing(fields2(2)),
////            ReplaceMissing(fields2(3)),
////            ReplaceMissing(fields2(4)),
////            ReplaceMissing(fields2(5)),
////            ReplaceMissing(fields2(6)),
////            ReplaceMissing(fields2(7)),
////            ReplaceMissing(fields2(8)),
////            ReplaceMissing(fields2(9)),
////            ReplaceMissing(fields2(10)),
////            ReplaceMissing(fields2(11)),
////            ReplaceMissing(fields2(12)),
////            ReplaceMissing(fields2(13)),
////            ReplaceMissing(fields2(14)),
////            ReplaceMissing(fields2(19)),
////            ReplaceMissing(fields2(20)),
////            ReplaceMissing(fields2(21)),
////            ReplaceMissing(fields2(22)),
////            codeWX(fields2(23)),
////            ReplaceMissing(fields2(24)),
////            ReplaceMissing(fields2(25)),
////            ReplaceMissing( fields2(26)),
////            ReplaceMissing(fields2(27)),
////            ReplaceMissing(fields2(28)),
////            ReplaceMissing(fields2(30)))
////  })
////    //Write to CSV
////    forecast.toDF.coalesce(1).write.format("csv").save("imputed_asos")
//    //forecast.toDF.write.format("csv").save("imputed_asos")
////
//    val schema = StructType(
//        StructField("longitude", FloatType, nullable = true) ::
//        StructField("latitude",  FloatType, nullable = true) ::
//        StructField("elevation", DoubleType, nullable = true) ::
//        StructField("tmpf",  FloatType, nullable = true) ::
//        StructField("dwpf",  FloatType, nullable = true) ::
//        StructField("relh",  FloatType, nullable = true) ::
//        StructField("drct",  FloatType, nullable = true) ::
//        StructField("sknt", DoubleType, nullable = true) ::
//        StructField("p01i",  FloatType, nullable = true) ::
//        StructField("alti",  FloatType, nullable = true) ::
//        StructField("mslp",  FloatType, nullable = true) ::
//        StructField("vsby",  FloatType, nullable = true) ::
//        StructField("gust", DoubleType, nullable = true) ::
//        StructField("skyl1",  DoubleType, nullable = true) ::
//        StructField("skyl2",  DoubleType, nullable = true) ::
//        StructField("skyl3",  DoubleType, nullable = true) ::
//        StructField("skyl4",  DoubleType, nullable = true) ::
//          StructField("wxcodes",  DoubleType, nullable = true) ::
//        StructField("ice_accretion_1hr",  FloatType, nullable = true) ::
//        StructField("ice_accretion_3hr",  FloatType, nullable = true) ::
//        StructField("ice_accretion_6hr",  FloatType, nullable = true) ::
//        StructField("peak_wind_gust", DoubleType, nullable = true) ::
//        StructField("peak_wind_drct", DoubleType, nullable = true) ::
//        StructField("feel",  FloatType, nullable = true) ::
//        Nil
//    )
////////    //Start Random Forest Algorithm
//////
////    //Pull CSV
//    val forecastDf = spark.read.format("csv")
//      .option("header", value = true)
//      .option("delimiter", ",")
//      .option("mode", "DROPMALFORMED")
//      .schema(schema)
//      .load("imputed_asos/part-00000-83cc32d0-5b11-4610-ab5d-7bcd91bc5ba9-c000.csv")
//      .cache()
//
//    RandomForest(forecastDf)
//
//    //forecastDf.describe("latitude").show()
//
//
//
//  }
//
//  def RandomForest(forecastDf: DataFrame): Unit ={
//    // Create Vectors
//    val cols = Array(
//      "tmpf",
//      "dwpf",
//      "relh",
//      "drct",
//      "sknt",
//      "alti",
//      "mslp",
//      "vsby",
//      "skyl1",
//      "feel")
//
//    val assembler = new VectorAssembler()
//      .setInputCols(cols)
//      .setOutputCol("features")
//    val featureDf = assembler.transform(forecastDf)
//
//    val indexer = new StringIndexer()
//      .setInputCol("wxcodes")
//      .setOutputCol("label")
//    val labelDf = indexer.fit(featureDf).transform(featureDf)
//
//    //  Random Forest
//    val seed = 5043
//
//    val Array(trainingData, testData) = labelDf.randomSplit(Array(0.7,0.3), seed)
//
//    val randomForestClassifier = new RandomForestClassifier()
//      .setImpurity("gini")
//      .setMaxDepth(8)
//      .setMaxBins(11)
//      .setNumTrees(64)
//      .setFeatureSubsetStrategy("auto")
//      .setSeed(seed)
//
//    val randomForestModel = randomForestClassifier.fit(trainingData)
//    //println(randomForestModel.toDebugString)
//
//
//    val predictionDf = randomForestModel.transform(testData)
//    //predictionDf.select("prediction", "label", "features").show(100)
//
//    val eval = new MulticlassClassificationEvaluator()
//      .setLabelCol("label")
//      .setPredictionCol("prediction")
//      .setMetricName("accuracy")
//    val acc = eval.evaluate(predictionDf)
//    println(acc * 100)
//    val error = 1.0-acc
//    println(s"Test Error = $error")
//  }
//
//
//  def GradientBoosting(data: DataFrame): Unit = {
//    //val data = spark.read.format("libsvm").load("data/mllib/sample_libsvm_data.txt")
//
//    // Index labels, adding metadata to the label column.
//    // Fit on whole dataset to include all labels in index.
//    val labelIndexer = new StringIndexer()
//      .setInputCol("label")
//      .setOutputCol("indexedLabel")
//      .fit(data)
//    // Automatically identify categorical features, and index them.
//    // Set maxCategories so features with > 4 distinct values are treated as continuous.
//    val featureIndexer = new VectorIndexer()
//      .setInputCol("features")
//      .setOutputCol("indexedFeatures")
//      .setMaxCategories(4)
//      .fit(data)
//
//    // Split the data into training and test sets (30% held out for testing).
//    val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))
//
//    // Train a GBT model.
//    val gbt = new GBTClassifier()
//      .setLabelCol("indexedLabel")
//      .setFeaturesCol("indexedFeatures")
//      .setMaxIter(10)
//
//    // Convert indexed labels back to original labels.
//    val labelConverter = new IndexToString()
//      .setInputCol("prediction")
//      .setOutputCol("predictedLabel")
//      .setLabels(labelIndexer.labels)
//
//    // Chain indexers and GBT in a Pipeline.
//    val pipeline = new Pipeline()
//      .setStages(Array(labelIndexer, featureIndexer, gbt, labelConverter))
//
//    // Train model. This also runs the indexers.
//    val model = pipeline.fit(trainingData)
//
//    // Make predictions.
//    val predictions = model.transform(testData)
//
//    // Select example rows to display.
//    //predictions.select("predictedLabel", "label", "features").show(5)
//
//    // Select (prediction, true label) and compute test error.
//    val evaluator = new MulticlassClassificationEvaluator()
//      .setLabelCol("indexedLabel")
//      .setPredictionCol("prediction")
//      .setMetricName("accuracy")
//    val accuracy = evaluator.evaluate(predictions)
//    println(accuracy * 100)
//    //println("Test Error = " + (1.0 - accuracy))
//  }
//
//  def XGBoostAlgorithm(data: DataFrame): Unit = {
//
//
//    val booster = new XGBoostClassifier(
//      //      Map("eta" -> 0.1f,
//      //        "max_depth" -> 2,
//      //        "objective" -> "multi:softprob",
//      //        "num_round" -> 100,
//      //        "num_workers" -> 3,
//      //        "num_classes" -> 2,
//      //        "tree_method" -> "auto"
//      //      )
//    )
//    booster.setFeaturesCol("features")
//    booster.setLabelCol("label")
//    booster.fit(data).transform(data).show()
//
//    // Batch prediction
//    //    val prediction = booster.transform(test)
//    //    prediction.show(false)
//    //
//    //    // Model evaluation
//    //    val evaluator = new MulticlassClassificationEvaluator()
//    //    evaluator.setLabelCol("label")
//    //    evaluator.setPredictionCol("prediction")
//    //    val accuracy = evaluator.evaluate(prediction)
//    //    println("The model accuracy is : " + accuracy)
//
//  }
//
//
//}
