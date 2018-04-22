# Move dataset to hadoop /tmp
#hadoop fs -put business.json /tmp

sc.version

from pyspark.sql import SQLContext sqlContext = SQLContext(sc)
#Pay attention to the name of SQLContext(sc)
business_df = sqlContext.read.format("json").load("/tmp/business.json")
review_df = sqlContext.read.format("json").load("/tmp/review.json")

print review_df.take(1)


print business_df.count()
print review_df.count()

#Print schema
review_df.printSchema()

#register the dataframe as table
business_df.registerTempTable("business_table")
review_df.registerTempTable("review_table")

#only save state, city, cacategories, name, business_id, review_count, 
#stars from business_table
business = sqlContext.sql("select state, city, categories, name, business_id, review_count, stars from business_table")


business.registerTempTable("business_table2")

#calculate the average review_count and stars per state
avg_business = 
business.groupBy("state").avg("review_count","stars").collect() for i in 
avg_business:
    print i


#join review and business by business_id join type: left outer join pay 
#attention to duplicate columns: stars and business_id
business_review = sqlContext.sql("select _corrupt_record, cool, date, funny, review_id, review_table.stars, text, useful, user_id, state, categories, name, review_table.business_id, review_count from review_table left outer join business_table2 on review_table.business_id = business_table2.business_id")

#add new year column, find out the date year range, then just retain the 
#nearest two year data
from pyspark.sql.functions import * newdf = 
business_review.withColumn("dt_year",year(business_review.date))
newdf.show(1)

#Print schema
business_review.printSchema()

#group by year
group_year = newdf.groupBy('dt_year').count("review_id") for i in 
group_year:
    print i

#get the nearest two years data
print newdf.filter(col('dt_year') > 2016).count()

review_2017 = newdf.filter(col('dt_year') > 2016)
review_2017.show(5)

#save the file, but there are too many files dur to partition
review_2017.write.mode('append').json("/tmp/review_2017.json")


