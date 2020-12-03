%python

#install prerequisite modules in Databricks
%pip install pyspark

from pyspark.sql.functions import col, countDistinct, isnan, avg, length, max, min

def categorical_summarized(df, dfName, limit):
  # spark SQL data summary functions
  # list of columns
  dfCol = df.columns
  rowCount = df.count()
  colCount = 1

  print('***** Analysis for Report {} ***** '.format(dfName))
  print('Row Count: {}'.format(rowCount))
  print('Column Count: {}'.format(len(dfCol)))
  print(df.printSchema())

  for col in dfCol:
    if rowCount == 0:
      print('Table has 0 row of data.')
      break
    print('*'*50)
    print('{}.'.format(colCount))
    print('Column Name: "{}"."{}"'.format(dfName,col))
    #get data type (may need new function to predict type)
    datatype = dict(df.dtypes)[col]
    print('Data Type: {}'.format(datatype))
    if datatype != 'timestamp' and datatype != 'boolean':
      # count and % of non-nulls (isnan does not work with TimeStamps or Boolean)
      nullCount = df.filter((df[col] == "") | df[col].isNull() | isnan(df[col])).count()
    else:
      nullCount = df.filter((df[col] == "") | df[col].isNull()).count()

    nonNullCount = rowCount - nullCount
    if nonNullCount != 0: #runs only if data rows are not null, nan, or blank
      nonNullpercent = round((rowCount - nullCount)/rowCount,2)
      print('Column Non-Null/NaN/Blank Count & %: {}, {}%'.format(nonNullCount,nonNullpercent*100))
      if datatype != 'boolean': #skips length analysis if data type is Boolean
        # unique value count and %
        # df.select(countDistinct(col)).show()
        uniqueCount = df.select(countDistinct(col)).collect()[0][0]
        print('Column Unique Count & %: {}, {}%'.format(uniqueCount, round(uniqueCount/nonNullCount*100,2)))
            # value string length statistics
        mincolLen = df.select(min(length(col))).collect()[0][0]
        maxcolLen = df.select(max(length(col))).collect()[0][0]
        avgcolLen = round(df.select(avg(length(col))).collect()[0][0],2)
        if mincolLen == maxcolLen == avgcolLen: #prints UNIFORM if all 3 fields are equal
          lenuniform = '- UNIFORM'
        else: 
          lenuniform = ''
        print('String Length Avg/Min/Max: {}, {}, {} {}'.format(avgcolLen, mincolLen, maxcolLen, lenuniform))
      else:
        uniqueCount = 2 #sets fixed value for Boolean data type
      # count of values (limits to top 10 or by limit)
      print('\nTop {} Distinct Values'.format(limit))
      df.groupBy(col).count().orderBy("count",ascending=False).limit(limit).show()
      if uniqueCount > limit: # skips bottom distinct value if not enough unique counts
        print('Bottom {} Distinct Values'.format(limit))
        df.groupBy(col).count().orderBy("count",ascending=True).limit(limit).show()
    else: #skips unnecessary processing if all fields are null, nan, or blank
      print('All values for this column are Null, Nan, or is blank.')

    print('*'*50)
    print('\n'*2)

    colCount +=1 #used for counting columns for print for each table
    
  print('***** End of Analysis for Report {}***** \n\n\n'.format(dfName))
  
  #imports table within Databricks and executes using function, dfName is the name of the table queried from
  df = sqlContext.sql('select * from {}'.format(dfName))
  categorical_summarized(df, dfName, 10)

#Will eventually need to build statistical packages for non-categorical value analysis
