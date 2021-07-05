package de.hpi.spark_tutorial

import org.apache.spark.sql.SparkSession

object Sindy {

  def discoverINDs(inputs: List[String], spark: SparkSession): Unit = {

    // TODO
    //Input Tuples --> Cells
    //  - Splitting each record with (value, (Singleton type))

    //Cells --> Cache-based Preaggregation (optional when repeatedly occuring cells with values)
    //- Should reduce network load
    //  - Groups by the value, and the singleton type

    //Cache-based Preaggregation --> Global partitioning
//    - Reordering the cells among the workers of the cluster through hashing
//    - We need an appropriate function to map each different value to unique cell
//    - Therefore: cells with the same value are on the same worker

//      Global partitioning --> Attribute Sets
//    - Grouping all cells by their values
//      - Aggregating attribute sets using union operator

//    Attribute Sets -->  Inclusion Lists
//    - Set with n attributes = n inclusion lists (all possible combinations)

//    Inclusion List --> Partition
//      - Group by the first attribute

//    Partition --> Aggregate
//    - Intersection with preaggregation
//    - Ends with attributes with empty sets; no (n,0)


//    Aggregate --> INDs
//    - Disassembling into INDs


  }
}
