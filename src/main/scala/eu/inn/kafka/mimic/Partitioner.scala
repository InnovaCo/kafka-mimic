package eu.inn.kafka.mimic

trait Partitioner {
  def resolvePartition(key: Array[Byte], sourcePartition: Int, targetPartitions: Int): Int
}

class KeepingPartitionPartitioner extends Partitioner {
  def resolvePartition(key: Array[Byte], sourcePartition: Int, targetPartitions: Int) = {
    if (sourcePartition >= targetPartitions) {
      throw new Exception(s"Partitions mismatch. No partition $sourcePartition in [0..${targetPartitions-1}].")
    }
    sourcePartition
  }
}

class KeepingOrderingPartitioner extends Partitioner {
  def resolvePartition(key: Array[Byte], sourcePartition: Int, targetPartitions: Int) =
    sourcePartition % targetPartitions
}

class SamzaFriendlyPartitioner extends Partitioner {
  def resolvePartition(key: Array[Byte], sourcePartition: Int, targetPartitions: Int) =
    Math.abs(new String(key, "utf-8").hashCode) % targetPartitions
}

class RandomPartitioner extends Partitioner {
  def resolvePartition(key: Array[Byte], sourcePartition: Int, targetPartitions: Int) =
    Math.floor(Math.random() * targetPartitions).toInt
}
