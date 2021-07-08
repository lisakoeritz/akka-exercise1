package de.hpi.spark_tutorial

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, collect_set, explode}

object Sindy {

  def discoverINDs(inputs: List[String], spark: SparkSession): Unit = {
    import spark.implicits._

    //1 - input data
    val datasets = inputs.map(filename => { //Reading csv files
      spark.read
        .option("inferSchema", "true")
        .option("header", "true")
        .option("delimiter", ";")
        .csv(filename)
    })
    //print(datasets)

    //2 - split each record into cells: tuples with a value and a singleton set -> (value, {column_name})
    val columnList = datasets.flatMap(data => data.columns.map(data.select(_))) //get columns from dataset into one dataset format
    //columnList.foreach(row => row.show())

    val tuples = columnList.map(column => column.map(row => (row.get(0).toString, row.schema.fieldNames.head)))
    val cells = tuples.reduce((d1, d2) => d1.union(d2)) // reduce to apply concatenation of datasets in list
    //cells.show()


    //3 - cache-based preaggregation: group by value and union attribute sets
    val preagg = cells.toDF("value", "attr_name")
      .groupBy("value")
      .agg(collect_set("attr_name").as("attributes"))
    //preagg.show()


    //4 - reorder all cells among the workers of the cluster
    // Spark automatically partitions RDDs

   
    // 5 - discard value, create attribute sets
    /*val attrSets = preagg.withColumn("attributes", explode(col("attributes")))
      .groupBy("value")
      .agg(collect_set("attributes").as("attr_set")) 
      .select("attr_set")*/ //deconstruct and reconstruct to merge all worker results: necessary??
    val attrSets = preagg.select("attributes").distinct().as[Seq[String]]
    //attrSets.show()

    // 6 - create inclusion lists
    val inclusionLists = attrSets
      .select(explode(col("attributes")).as("key"), col("attributes"))
      .as[(String, Seq[String])]
      .map(row => (row._1, row._2.filter(_ != row._1)))

    //inclusionLists.show()

    // 7 - group by key + intersect attribute sets
    val aggInclList = inclusionLists.toDF("key", "ind_candidates")
      .groupBy("key")
      .agg(collect_set("ind_candidates").as("ind_candidates")).as[(String, Seq[Seq[String]])]
    //aggInclList.show()

    val intersectedInclList = aggInclList.map(row => (row._1, row._2.reduce(_.intersect(_)))) //does this make sense?!
    val filteredINDList = intersectedInclList.filter(row => row._2.nonEmpty)
    //filteredINDList.show()


    // 8 - sort and output INDs
    val output = filteredINDList.toDF("dependent", "referenced_IND").sort("dependent").as[(String,Seq[String])]
    //output.show()

    output.collect().foreach(row => println(row._1 + " > " + row._2.reduce(_ + " , " + _)))


  }
}
