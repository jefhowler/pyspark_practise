from pyspark.sql import SparkSession
from pyspark.sql import functions as F

class DataFrameProcessor:
    def __init__(self):
        self.spark = SparkSession.builder.appName("Data Frame Processor").getOrCreate()
        self.dataFrame = self.spark.read.csv('./SampleCSV.csv', inferSchema=True, header=True)


    def rowFilter(self, columnName, value):
        filteredRows = self.dataFrame.filter(self.dataFrame[columnName] == value)

        return filteredRows
    

    def columnFilter(self, columnName):
        filteredColumn = self.dataFrame.select(columnName)

        return filteredColumn
    
    def columnMean(self, columnName):
        columnMean = self.dataFrame.agg(F.mean(columnName)).collect()[0][0]
        return columnMean



processor = DataFrameProcessor()
filteredRow = processor.rowFilter('Name', 'John Doe')
filteredRow.show()
filteredColumn = processor.columnFilter('Department')
filteredColumn.show()
averageSalary = processor.columnMean('Salary')
print(averageSalary)
