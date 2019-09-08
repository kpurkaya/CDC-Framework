from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions

if __name__ == "__main__":
    # Create a SparkSession # provide the cluster information unless running on local system.
    spark = SparkSession.builder.master("local[*]").appName("SCD_Framework").getOrCreate()

    spark.conf.set("spark.debug.maxToStringFields", 250)

    # Create the dataframe from u.item file
    martFrame = spark.read.load("ml-100k/u.item", format="csv", sep="|", inferSchema="true", header="false")
    martFrame.show()

    srcFrame = spark.read.load("ml-100k/u_delta.item", format="csv", sep="|", inferSchema="true", header="false")
    srcFrame.show()

    # create temp views from Mart and Source(Delta file)
    martFrame.createOrReplaceTempView("martView")
    srcFrame.createOrReplaceTempView("srcView")

    spark.sql("select * from martView").show()
    spark.sql("select * from srcView").show()

    # create the History DataFrame
    spark.sql("select 'I' as dlta_flag, current_timestamp() as hist_crt_date, martView.* from martView").show()

    # use cache to capture correct timestamp for hst_crt_date in order for CDC to work
    histFrame = spark.sql(
        "select 'I' as dlta_flag, current_timestamp() as hist_crt_date, martView.* from martView").cache()
    histFrame.createOrReplaceTempView("histView")

    # create the Delta DataFrame
    spark.sql(
        "select CASE WHEN martView._c0 IS NULL THEN 'I' END as dlta_flag, current_timestamp() as hist_crt_date, srcView.* from srcView left join martView on srcView._c0 = martView._c0 where martView._c0 is null  UNION select CASE WHEN martView._c0 =srcView._c0 THEN 'U' END as dlata_flag, current_timestamp() as hist_crt_date, srcView.* from srcView left join martView on srcView._c0 = martView._c0 where (srcView._c1 <> martView._c1 or srcView._c2 <> martView._c2 )").show()
    deltaFrame = spark.sql(
        "select CASE WHEN martView._c0 IS NULL THEN 'I' END as dlta_flag, current_timestamp() as hist_crt_date, srcView.* from srcView left join martView on srcView._c0 = martView._c0 where martView._c0 is null  UNION select CASE WHEN martView._c0 =srcView._c0 THEN 'U' END as dlata_flag, current_timestamp() as hist_crt_date, srcView.* from srcView left join martView on srcView._c0 = martView._c0 where (srcView._c1 <> martView._c1 or srcView._c2 <> martView._c2 )").cache()
    deltaFrame.createOrReplaceTempView("deltaView")

    # create the new History DataFrame that combines existing and newly identified Delta
    newHistFrame = spark.sql("select * from histView UNION select * from deltaView")
    newHistFrame.createOrReplaceTempView("newHistView")
    spark.sql("select * from newHistView order by hist_crt_date desc, _c0 ").show()

    spark.sql(
        "select dlta_flag,_c0,hist_crt_date, rank() over (partition by _c0 order by hist_crt_date) as rnk from newHistView").show()
    newHistFrameTemp = spark.sql(
        "select dlta_flag,hist_crt_date, rank() over (partition by _c0 order by hist_crt_date desc) as rnk, _c0,_c1,_c2,_c3,_c4,_c5,_c6,_c7,_c8,_c9,_c10,_c11,_c12,_c13,_c14,_c15,_c16,_c17,_c18,_c19,_c20,_c21,_c22,_c23 from newHistView order by _c0")
    newHistFrameTemp.createOrReplaceTempView("newHistFrameTempView")
    spark.sql("select * from newHistFrameTempView where rnk = 1")
    spark.sql("select * from newHistFrameTempView order by _c0, hist_crt_date, rnk").show(truncate=False)

    # create the new Mart DataFrame that has the latest updates from the Delta also including new records from source.
    newMartFrame = spark.sql(
        "select _c0,_c1,_c2,_c3,_c4,_c5,_c6,_c7,_c8,_c9,_c10,_c11,_c12,_c13,_c14,_c15,_c16,_c17,_c18,_c19,_c20,_c21,_c22,_c23 from newHistFrameTempView where rnk = 1")
    newMartFrame.createOrReplaceTempView("newMartFrameView")
    spark.sql("select * from newMartFrameView").show()
    spark.sql("select * from newMartFrameView order by _c0 desc").show()

    # create the Final History Dataframe that has the new Delta captured into existing History.
    # in the final History file you do not need the rank column
    finalHistFrame = spark.sql(
        "select dlta_flag,hist_crt_date, _c0,_c1,_c2,_c3,_c4,_c5,_c6,_c7,_c8,_c9,_c10,_c11,_c12,_c13,_c14,_c15,_c16,_c17,_c18,_c19,_c20,_c21,_c22,_c23 from newHistFrameTempView ")
    finalHistFrame.createOrReplaceTempView("finalHistFrameView")
    spark.sql("select * from finalHistFrameView").show()
    spark.sql("select * from finalHistFrameView order by _c0 desc").show()

    # Write the new Mart and Updated History to Data files # use your appropriate hadoop infrastructure settings below
    #newMartFrame.write.format("csv").save("U_mart.item").mode("overwrite").option("header", "false").sep("|")
    #finalHistFrame.write.format("csv").save("U_Hist.item").mode("overwrite").option("header", "false").sep("|")

    # Stop the session
    spark.stop()
