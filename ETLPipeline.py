from pyspark.sql import SparkSession
from pyspark.sql import functions as F

class ETLPipeline:
    def __init__(self, filePath):
        self.spark = SparkSession.builder.appName("ETL Pipeline").getOrCreate()
        self.dataFrame = self.spark.read.csv(filePath, inferSchema=True, header=True)
        self.transformedDataFrame = ''

    def transformData(self, transformationFunction, *args):
         self.transformedDataFrame = transformationFunction(self.dataFrame, *args)

    def outputTranformedDataToNewCSV(self, filepath):
        self.transformedDataFrame.repartition(1).write.csv(filepath, header=True)

   
    def stopSpark(self):
        self.spark.stop()
    

def countPerUniqueValueInColumn(dataFrame, column, nameOfCountColumn='count'):
        counts = dataFrame.groupBy(column).count()
        if nameOfCountColumn != 'count':
            counts = counts.withColumnRenamed('count', nameOfCountColumn)

        return counts
    
pipeline = ETLPipeline('./ETLSampleData.csv')
pipeline.transformData(countPerUniqueValueInColumn,'product_id', 'total_quantity')
pipeline.outputTranformedDataToNewCSV('./totalSalesByProduct.csv')
pipeline.stopSpark()
