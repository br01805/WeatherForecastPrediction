//import ml.dmlc.xgboost4j.scala.spark.XGBoostClassifier
import org.apache.spark._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.ml.classification.{GBTClassifier, LogisticRegression, NaiveBayesModel, RandomForestClassifier}
import org.apache.spark.ml.feature.{Imputer, IndexToString, StringIndexer, VectorAssembler, VectorIndexer}
import org.apache.spark.sql.{DataFrame, SQLContext, SQLImplicits, SparkSession}
import org.apache.spark.sql.types.{DoubleType, FloatType, IntegerType, StringType, StructField, StructType}
import shapeless.syntax.std.tuple.productTupleOps
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.functions.collect_list
import spire.random.Random.{double, long}

import scala.reflect.ClassManifestFactory.Long

object ForecastPrediction {
  private[this] val schema = StructType(
      StructField("tmpf",  FloatType, nullable = true) ::
      StructField("dwpf",  FloatType, nullable = true) ::
      StructField("relh",  FloatType, nullable = true) ::
      StructField("drct",  FloatType, nullable = true) ::
      StructField("sknt", DoubleType, nullable = true) ::
      StructField("alti",  FloatType, nullable = true) ::
      StructField("mslp",  FloatType, nullable = true) ::
      StructField("vsby",  FloatType, nullable = true) ::
      StructField("skyl1",  DoubleType, nullable = true) ::
      StructField("feel",  FloatType, nullable = true) ::
      StructField("wxcodes",  DoubleType, nullable = true) ::
      Nil
  )

  def codeWX(wxcode: String): String ={
    if (List("RA", "DZ", "PCPN").exists(wxcode.contains(_))){
      return "1";
    }
    if (List("SN", "SP", "SG", "S").exists(wxcode.contains(_))){
      return "2";
    }
    if (List("FZRA", "IC", "GR", "GS", "PL").exists(wxcode.contains(_))){
      return "3";
    }
    "0";
  }

  def checkWX(wxcode: String): Boolean ={ // I just duplicated the above code, yes all this code is unnecessary
    val keys = List("RA", "DZ", "PCPN", "SN", "SP", "SG", "S", "FZRA", "IC", "GR", "GS", "PL")
    keys.exists(wxcode.contains(_))
  }

  def ReplaceMissing(field: String, replaceNegative: Boolean = false): String ={
    if (field == ("M") || field == ("T") || (replaceNegative && field.startsWith("-"))){
      return "0"
    }
    field
  }

  def build_csv(spark: SparkSession, sc: SparkContext): Unit = {
    // val forecastCSV = sc.textFile("asos_1976_1979.csv") read in single file
    val SQLContext = new SQLContext(sc)
    import SQLContext.implicits._
    val forecastCSV = List("Weather_sets/asos_1976_1979.csv", "Weather_sets/asos_1980_1989.csv", "Weather_sets/asos_1990_1999.csv", "Weather_sets/asos_2000_2010.csv", "Weather_sets/asos_2011_2022.csv")
      .map(spark.sparkContext.textFile(_))
      .reduce(_ union _)
    //FilterDataset
    val filteredCSV = filterDataset(forecastCSV)
    filteredCSV.toDF.coalesce(10).write.format("csv").save("asos_combined_coded")
  }

  def build_rdd(spark: SparkSession, sc: SparkContext): RDD[WeatherModifiedCSVMapping] = {
    // val forecastCSV = sc.textFile("asos_1976_1979.csv") read in single file
    val SQLContext = new SQLContext(sc)
    import SQLContext.implicits._
    val forecastCSV = List("Weather_sets/asos_1976_1979.csv", "Weather_sets/asos_1980_1989.csv", "Weather_sets/asos_1990_1999.csv", "Weather_sets/asos_2000_2010.csv", "Weather_sets/asos_2011_2022.csv")
      .map(spark.sparkContext.textFile(_))
      .reduce(_ union _)
    //FilterDataset
    val filteredCSV = filterDataset(forecastCSV)
    filteredCSV
  }


  def read_csv(spark: SparkSession, path: String): DataFrame ={
    spark.read.format("csv")
      .option("header", value = false)
      .option("delimiter", ",")
      .option("mode", "DROPMALFORMED")
      .schema(schema)
      .load(path)
      .cache()
  }

  def time[R](block: => Double): (Double, Double) = {
    val t0 = System.nanoTime()
    val result = block
    val t1 = System.nanoTime()
    //println("Estimated time: " + (t1 - t0) + "ns")
    val seconds  = (t1-t0)/1E9
    (result, seconds)
  }

  def runAnalysisDF(size: String,  spark: SparkSession, callback: (DataFrame) => Double, name: String): Unit ={
    println(s"--------${name} (5 RUNS)------------")
    val run1 = spark.read.format("libsvm").load(s"clean_sets/$size/svm/{$size}_svm_1.txt")
    val run2 = spark.read.format("libsvm").load(s"clean_sets/$size/svm/{$size}_svm_2.txt")
    val run3 = spark.read.format("libsvm").load(s"clean_sets/$size/svm/{$size}_svm_3.txt")
    val run4 = spark.read.format("libsvm").load(s"clean_sets/$size/svm/{$size}_svm_4.txt")
    val run5 = spark.read.format("libsvm").load(s"clean_sets/$size/svm/{$size}_svm_5.txt")
    val Acc1 = time { callback(run1) }
    var lowest = Acc1._1
    var highest = Acc1._1
    println(s"Run 1: ${Acc1._1}, Time: ${Acc1._2}")
    val Acc2 = time { callback(run2) }
    lowest = if (Acc2._1 < lowest) Acc2._1 else lowest
    highest = if (Acc2._1 > highest) Acc2._1 else highest
    println(s"Run 2: ${Acc2._1}, Time: ${Acc2._2}")
    val Acc3 = time { callback(run3) }
    lowest = if (Acc3._1 < lowest) Acc3._1 else lowest
    highest = if (Acc3._1 > highest) Acc3._1 else highest
    println(s"Run 3: ${Acc3._1}, Time: ${Acc3._2}")
    val Acc4 = time { callback(run4) }
    lowest = if (Acc4._1 < lowest) Acc4._1 else lowest
    highest = if (Acc4._1 > highest) Acc4._1 else highest
    println(s"Run 4: ${Acc4._1}, Time: ${Acc4._2}")
    val Acc5 = time { callback(run5) }
    lowest = if (Acc5._1 < lowest) Acc5._1 else lowest
    highest = if (Acc5._1 > highest) Acc5._1 else highest
    println(s"Run 5: ${Acc5._1}, Time: ${Acc5._2}")
    println(s"-------${name} ANALYSIS-------------")
    val avg = (Acc1._1 + Acc2._1 + Acc3._1 + Acc4._1 + Acc5._1)/5
    val avg_time = (Acc1._2 + Acc2._2 + Acc3._2 + Acc4._2 + Acc5._2)/5
    println(s"Average Accuracy: ${avg}")
    println(s"Lowest Accuracy: ${lowest}")
    println(s"Highest Accuracy: ${highest}")
    println(s"Average Time: ${avg_time} ")
    println("-------------------------------------")
  }

  def runAnalysisSVM(size: String,  sc: SparkContext, callback: (RDD[LabeledPoint]) => Double, name: String): Unit ={
    println(s"--------${name} (5 RUNS)------------")
    val run1 = MLUtils.loadLibSVMFile(sc,s"clean_sets/$size/svm/{$size}_svm_1.txt")
    val run2 = MLUtils.loadLibSVMFile(sc,s"clean_sets/$size/svm/{$size}_svm_2.txt")
    val run3 = MLUtils.loadLibSVMFile(sc,s"clean_sets/$size/svm/{$size}_svm_3.txt")
    val run4 = MLUtils.loadLibSVMFile(sc,s"clean_sets/$size/svm/{$size}_svm_4.txt")
    val run5 = MLUtils.loadLibSVMFile(sc,s"clean_sets/$size/svm/{$size}_svm_5.txt")
    val Acc1 = time { callback(run1) }
    var lowest = Acc1._1
    var highest = Acc1._1
    println(s"Run 1: ${Acc1._1}, Time: ${Acc1._2}")
    val Acc2 = time { callback(run2) }
    lowest = if (Acc2._1 < lowest) Acc2._1 else lowest
    highest = if (Acc2._1 > highest) Acc2._1 else highest
    println(s"Run 2: ${Acc2._1}, Time: ${Acc2._2}")
    val Acc3 = time { callback(run3) }
    lowest = if (Acc3._1 < lowest) Acc3._1 else lowest
    highest = if (Acc3._1 > highest) Acc3._1 else highest
    println(s"Run 3: ${Acc3._1}, Time: ${Acc3._2}")
    val Acc4 = time { callback(run4) }
    lowest = if (Acc4._1 < lowest) Acc4._1 else lowest
    highest = if (Acc4._1 > highest) Acc4._1 else highest
    println(s"Run 4: ${Acc4._1}, Time: ${Acc4._2}")
    val Acc5 = time { callback(run5) }
    lowest = if (Acc5._1 < lowest) Acc5._1 else lowest
    highest = if (Acc5._1 > highest) Acc5._1 else highest
    println(s"Run 5: ${Acc5._1}, Time: ${Acc5._2}")
    println(s"-------${name} ANALYSIS-------------")
    val avg = (Acc1._1 + Acc2._1 + Acc3._1 + Acc4._1 + Acc5._1)/5
    val avg_time = (Acc2._1 + Acc2._2 + Acc3._2 + Acc4._2 + Acc5._2)/5
    println(s"Average Accuracy: ${avg}")
    println(s"Lowest Accuracy: ${lowest}")
    println(s"Highest Accuracy: ${highest}")
    println(s"Average Time: ${avg_time} ")
    println("-------------------------------------")
  }

  def main(args: Array[String]): Unit ={
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sparkConf = new SparkConf().setAppName("Asos")
      .set("spark.executor.memory", "64g")
      .set("spark.driver.memory", "64g")
      .setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    val spark = SparkSession.builder().appName("Asos")
      .config("spark.executor.memory", "64g")
      .config("spark.driver.memory", "64g")
      .config("master", "local[*]").getOrCreate()

    // Build Datasets
    //build_csv(spark, sc);
    //val rdd = build_rdd(spark, sc);

    runAnalysisSVM("10k", sc, NaiveBayesAlgorithm, "Naive Bayes 10k")
    runAnalysisSVM("25k", sc, NaiveBayesAlgorithm, "Naive Bayes 25k")
    runAnalysisSVM("50k", sc, NaiveBayesAlgorithm, "Naive Bayes 50k")

    runAnalysisSVM("10k", sc, DecisionTreeAlgorithm, "Decision Tree 10k")
    runAnalysisSVM("25k", sc, DecisionTreeAlgorithm, "Decision Tree 25k")
    runAnalysisSVM("50k", sc, DecisionTreeAlgorithm, "Decision Tree 50k")

    runAnalysisDF("10k", spark, RandomForestSVM, "Random Forest 10k")
    runAnalysisDF("25k", spark, RandomForestSVM, "Random Forest 25k")
    runAnalysisDF("50k", spark, RandomForestSVM, "Random Forest 50k")
  }

case class AnalysisCSVMapping(
                               algorithm: String,
                               run: String
                             )

case class WeatherModifiedCSVMapping(
                                      tmpf: String,
                                      dwpf: String,
                                      relh: String,
                                      drct: String,
                                      sknt: String,
                                      alti: String,
                                      mslp: String,
                                      vsby: String,
                                      skyl1: String,
                                      feel: String,
                                      wxcodes: String
                                    )

  def filterDataset(RDD: RDD[String]): RDD[WeatherModifiedCSVMapping] = {
    val header = RDD.first()
    val forecast = RDD.filter(row => row != header).filter(row => {
      val fields = row.split(",").map(_.trim)
      (fields(2) != ("M") &&
         checkWX(fields(20)))
    }).map(row => {
      println(row)
      val fields2 = row.split(",").map(_.trim)
      WeatherModifiedCSVMapping(
        ReplaceMissing(fields2(2), replaceNegative = true),
        ReplaceMissing(fields2(3)),
        ReplaceMissing(fields2(4)),
        ReplaceMissing(fields2(5)),
        ReplaceMissing(fields2(6)),
        ReplaceMissing(fields2(8)),
        ReplaceMissing(fields2(9)),
        ReplaceMissing(fields2(10)),
        ReplaceMissing(fields2(16)),
        ReplaceMissing(fields2(27), replaceNegative = true),
        codeWX(fields2(20)))
    })
    forecast
  }

  def NaiveBayesAlgorithm(data: RDD[LabeledPoint]): Double = {
    val Array(training, test) = data.randomSplit(Array(0.7, 0.3))

    val model = NaiveBayes.train(training, lambda = 1.0, modelType = "multinomial")

    val predictionAndLabel = test.map(p => (model.predict(p.features), p.label))
    val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / test.count()
    val acc = accuracy * 100
    acc
  }

  def RandomForestSVM(data: DataFrame): Double = {
    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
      .fit(data)
    // Automatically identify categorical features, and index them.
    // Set maxCategories so features with > 4 distinct values are treated as continuous.
    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(4)
      .fit(data)

    // Split the data into training and test sets (30% held out for testing).
    val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))

    // Train a RandomForest model.
    val rf = new RandomForestClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("indexedFeatures")
      .setNumTrees(10)

    // Convert indexed labels back to original labels.
    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)

    // Chain indexers and forest in a Pipeline.
    val pipeline = new Pipeline()
      .setStages(Array(labelIndexer, featureIndexer, rf, labelConverter))

    // Train model. This also runs the indexers.
    val model = pipeline.fit(trainingData)

    // Make predictions.
    val predictions = model.transform(testData)

//    val confusionMatrix = predictions.
//      groupBy("indexedLabel").
//      pivot("prediction", (1 to 3)).
//      count().
//      na.fill(0.0).
//      orderBy("indexedLabel")
//
//    confusionMatrix.show()

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(predictions)
    val acc = (accuracy * 100)
    acc
    //println("Test Error = " + (1.0 - accuracy))

  }

  def DecisionTreeAlgorithm(data: RDD[LabeledPoint]): Double = {
    // Split the data into training and test sets (30% held out for testing)
    val splits = data.randomSplit(Array(0.7, 0.3))
    val (trainingData, testData) = (splits(0), splits(1))

    // Train a DecisionTree model.
    //  Empty categoricalFeaturesInfo indicates all features are continuous.
    val numClasses = 4
    val categoricalFeaturesInfo = Map[Int, Int]()
    val impurity = "gini"
    val maxDepth = 8
    val maxBins = 11

    val model = DecisionTree.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
      impurity, maxDepth, maxBins)

    // Evaluate model on test instances and compute test error
    val labelAndPreds = testData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    val accuracy = 1.0 * labelAndPreds.filter(x => x._1 == x._2).count() / testData.count()
    val acc = accuracy * 100
    acc
  }
}



