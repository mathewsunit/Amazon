package Graph

import java.io.{BufferedWriter, OutputStreamWriter}

import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Amazon {
  def main(args: Array[String]) {

    if (args.length < 2) {
      System.out.println("Usage: <JarName> <Input> <Output>")
      return
    }

    val input = args(0).toString

    val sparkConfiguration = new SparkConf().
      setAppName("spark-twitter-stream").
      setMaster(sys.env.get("spark.master").getOrElse("local[*]"))
    val sc = new SparkContext(sparkConfiguration)

    val fs = FileSystem.get(sc.hadoopConfiguration)
    val path: Path = new Path(args(1))
    if (fs.exists(path)) {
      fs.delete(path, true)
    }
    val dataOutputStream: FSDataOutputStream = fs.create(path)
    val bw: BufferedWriter = new BufferedWriter(new OutputStreamWriter(dataOutputStream, "UTF-8"))

    val sparkRdd = sc.textFile(input)

    val interRdd = sparkRdd
      .filter(line => !line.contains("#"))

    val vertices: RDD[(VertexId, String)] = interRdd
      .flatMap(x => x.split("\\s+"))
      .map(x => (x.toInt.asInstanceOf[Number].longValue, x.asInstanceOf[String]))
      .distinct()
    val edges:RDD[Edge[Long]] = interRdd
      .map(x => x.split("\\s+"))
      .map(row => Edge(row(0).toInt.asInstanceOf[Number].longValue, row(1).toInt.asInstanceOf[Number].longValue, 1))

    val graph = Graph(vertices, edges)
    graph.cache()

    bw.write("\n")
    bw.write("Total Number of Vs: " + graph.numVertices)
    bw.write("\n")
    bw.write("Total Number of Es: " + graph.numEdges)
    bw.write("\n")

    // In Degree
    bw.write("\n")
    bw.write("In degrees: ")
    bw.write("\n")
    graph
      .inDegrees // computes in Degrees
      .join(vertices)
      .sortBy(_._2._1, ascending=false)
      .take(5)
      .foreach(x => bw.write(x._2._2 + " has " + x._2._1 + " in degrees.\n"))

    // Out Degree
    bw.write("\n")
    bw.write("Out degrees: ")
    bw.write("\n")
    graph
      .outDegrees
      .join(vertices)
      .sortBy(_._2._1, ascending=false)
      .take(5)
      .foreach(x => bw.write(x._2._2 + " has " + x._2._1 + " out degrees.\n"))

    // Page Rank
    bw.write("\n")
    bw.write("Page Rank: ")
    bw.write("\n")
    val ranks = graph.pageRank(0.0001).vertices
    ranks
      .join(vertices)
      .sortBy(_._2._1, ascending=false) // sort by the rank
      .take(5) // get the top 10
      .foreach(x => bw.write(x._2._2+"\n"))

    // WCC
    bw.write("\n")
    bw.write("WCC: ")
    bw.write("\n")
    val scc = graph.connectedComponents()

    vertices.join(scc.vertices).map {
      case (id, (vertex, cc)) => (cc->1)
    }.reduceByKey(_+_)
      .sortBy(_._1, ascending=false) // sort by the rank
      .take(5) // get the top 10
      .foreach(x => bw.write("["+x._1+","+x._2+"]\n"))

    // Triangles
    bw.write("\n")
    bw.write("Triangles: ")
    bw.write("\n")
    val tri = graph.triangleCount()

    vertices.join(tri.vertices).map {
      case (id, (vertex, tc)) =>
        (vertex, tc)
    }.reduceByKey(_+_)
      .sortBy(_._2, ascending=false) // sort by the rank
      .take(5) // get the top 10
      .foreach(x => bw.write("["+x._1+","+x._2+"]\n"))

    bw.close
  }
}
