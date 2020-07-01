"""
数据集：http://archive.ics.uci.edu/ml/datasets/Adult
PCA主成分提取
"""

from pyspark.ml.feature import PCA
from pyspark.sql import Row
from pyspark.ml.linalg import Vectors
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import IndexToString, StringIndexer, VectorIndexer, HashingTF, Tokenizer
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.classification import LogisticRegressionModel
from pyspark.ml.classification import BinaryLogisticRegressionSummary, LogisticRegression
from pyspark.sql import functions
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

conf = SparkConf().setMaster("local").setAppName("MyApp")
sc = SparkContext(conf=conf)
spark = SparkSession.builder.config(conf=SparkConf()).getOrCreate()


# 获取训练集测试集
def f(x):
    rel = {}
    try:
        rel['features'] = Vectors.dense(float(x[0].strip(' ')),
                                        float(x[2].strip(' ')),
                                        float(x[4].strip(' ')),
                                        float(x[10].strip(' ')),
                                        float(x[11].strip(' ')),
                                        float(x[12].strip(' ')))
    except:
        rel['features'] = Vectors.dense(0, 0, 0, 0, 0, 0)
    rel['label'] = str(x[14].strip('.'))
    return rel


df = spark.sparkContext.textFile('./adult/adult.data').map(lambda line: line.split(',')).\
    map(lambda x: Row(**f(x))).toDF()

test = spark.sparkContext.textFile('./adult/adult.test').map(lambda line: line.split(',')).\
    map(lambda x: Row(**f(x))).toDF()

# 构建模型
pca = PCA(k=3, inputCol='features', outputCol='pcaFeatures').fit(df)

result = pca.transform(df)

test_data = pca.transform(test)

result.show(truncate=False)
test_data.show(truncate=False)

# 在主成分分析的基础上做逻辑回归
labelIndexer = StringIndexer(inputCol='label', outputCol='indexedLabel').fit(result)
for label in labelIndexer.labels:
    print(label)

featureIndexer = VectorIndexer(inputCol='pcaFeatures', outputCol='indexedFeatures').fit(result)
print(featureIndexer.numFeatures)

labelConverter = IndexToString(inputCol='prediction', outputCol='predictedLabel', labels=labelIndexer.labels)

lr = LogisticRegression().setLabelCol('indexedLabel').setFeaturesCol('indexedFeatures').setMaxIter(100)

lrPipeline = Pipeline().setStages([labelIndexer, featureIndexer, lr, labelConverter])

lrPipelineModel = lrPipeline.fit(result)

lrModel = lrPipelineModel.stages[2]

print("Coefficients: \n " + str(lrModel.coefficientMatrix) +
      "\nIntercept: " + str(lrModel.interceptVector) +
      "\n numClasses: " + str(lrModel.numClasses) +
      "\n numFeatures: " + str(lrModel.numFeatures))

lrPredictions = lrPipelineModel.transform(test_data)

# 评估
evaluator = MulticlassClassificationEvaluator().setLabelCol('indexedLabel').setPredictionCol('prediction')

lrAccuracy = evaluator.evaluate(lrPredictions)
print("Test Error = %g " % (1.0 - lrAccuracy))

# 超参调优
pca = PCA().setInputCol('features').setOutputCol('pcaFeatures')
labelIndexer = StringIndexer(inputCol='label', outputCol='indexedLabel').fit(df)
featureIndexer = VectorIndexer(inputCol='pcaFeatures', outputCol='indexedFeatures')
labelConverter = IndexToString(inputCol='prediction', outputCol='predictedLabel', labels=labelIndexer.labels)
lr = LogisticRegression().setLabelCol('indexedLabel').setFeaturesCol('indexedFeatures').setMaxIter(100)
lrPipeline = Pipeline().setStages([pca, labelIndexer, featureIndexer, lr, labelConverter])
paramGrid = ParamGridBuilder().addGrid(pca.k, [1, 2, 3, 4, 5, 6]).\
                               addGrid(lr.elasticNetParam, [0.2, 0.8]).\
                               addGrid(lr.regParam, [0.01, 0.1, 0.5]).build()
cv = CrossValidator().setEstimator(lrPipeline).\
    setEvaluator(MulticlassClassificationEvaluator().setLabelCol('indexedLabel').setPredictionCol('prediction')).\
    setEstimatorParamMaps(paramGrid).\
    setNumFolds(3)
cvModel = cv.fit(df)

lrPredictions = cvModel.transform(test)
evaluator = MulticlassClassificationEvaluator().setLabelCol('indexedLabel').setPredictionCol('prediction')
lrAccuracy = evaluator.evaluate(lrPredictions)
print('best test error: ', 1-lrAccuracy)  # 最佳模型的结果
bestModel = cvModel.bestModel
lrModel = bestModel.stages[3]

print('Coefficients:\n' + str(lrModel.coefficientMatrix)+
     "\n Intercept: " + str(lrModel.interceptVector)+
     "\n numClasses: " + str(lrModel.numClasses)+
     "\n numFeatures: " + str(lrModel.numFeatures))
