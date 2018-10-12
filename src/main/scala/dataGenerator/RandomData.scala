package dataGenerator

import java.nio.ByteBuffer
import java.util

import org.apache.avro.Schema
import org.apache.avro.Schema.Type
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8

import scala.collection.JavaConversions._
import scala.util.Random

class RandomData(schema: Schema, countRecord: Int, seed: Long,
                 fieldsWithCustomValue: Map[String, List[String]]) extends Iterable[Any] {

  /*
   * A secondary constructor.
   */
  def this(schema: Schema, countRecord: Int, fieldsWithCustomValue: Map[String, List[String]]) {
    this(schema, countRecord, System.currentTimeMillis(), fieldsWithCustomValue)
  }

  override def iterator: Iterator[Any] = {
    new RandomDataIterator
  }

  private def generate(schema: Schema, random: Random, d: Int): Any = {
    val TIMESTAMP_TO_START: Long = 1443866555

    schema.getType match {
      case Type.RECORD =>
        val record = new GenericData.Record(schema)
        for (entry <- schema.getFields.toList) {
          if(fieldsWithCustomValue.contains(entry.name)){
            record.put(entry.name, randomCustomValue(random, entry.name))
          } else {
            record.put(entry.name, generate(entry.schema, random, d + 1))
          }
        }
        record
      case Type.ENUM =>
        val symbols = schema.getEnumSymbols
        symbols.get(random.nextInt(symbols.size))
      case Type.ARRAY =>
        val length = (random.nextInt(5) + 2) - d
        val array = new GenericData.Array[Any](if (length <= 0) 0 else length, schema)
        for (i <- 0 until length) {
          array.add(generate(schema.getElementType, random, d + 1))
        }
        array
      case Type.MAP =>
        val length = (random.nextInt(5) + 2) - d
        val map = new util.HashMap[Any, Any](if (length <= 0) 0 else length)
        for (i <- 0 until length) {
          map.put(randomUtf8(random, 20), generate(schema.getValueType, random, d + 1))
        }
        map
      case Type.UNION =>
        val types = schema.getTypes
        generate(types.get(random.nextInt(types.size)), random, d)
      case Type.FIXED =>
        val bytes = new Array[Byte](schema.getFixedSize)
        random.nextBytes(bytes)
        new GenericData.Fixed(schema, bytes)
      case Type.STRING =>
        randomUtf8(random, 20)
      case Type.BYTES =>
        randomBytes(random, 20)
      case Type.INT =>
        Math.abs(random.nextInt)
      case Type.LONG =>
        val randomInt = TIMESTAMP_TO_START + Math.abs(random.nextInt())
        Math.abs(System.currentTimeMillis() - randomInt)
      case Type.FLOAT =>
        Math.abs(random.nextFloat)
      case Type.DOUBLE =>
        Math.abs(random.nextDouble)
      case Type.BOOLEAN =>
        random.nextBoolean
      case Type.NULL =>
        null
      case _ =>
        throw new RuntimeException("Unknown type: " + schema)
    }
  }

  private def randomCustomValue(random: Random, key: String) = {
    val value = fieldsWithCustomValue(key)

    value(random.nextInt(value.size))
  }

  private def randomUtf8(random: Random, maxLenght: Int): Utf8 = {
    val rand = StringBuilder.newBuilder
    for (i <- 0 until maxLenght){
      rand.append((97 + random.nextInt(25)).toChar)
    }
    new Utf8().set(rand.toString())
  }

  private def randomBytes(rand: Random, maxLength: Int): ByteBuffer = {
    val bytes = ByteBuffer.allocate(rand.nextInt(maxLength))
    bytes.limit(bytes.capacity)
    rand.nextBytes(bytes.array)
    bytes
  }

  class RandomDataIterator extends Iterator[Any] {
    private var n: Int = 0
    private val random = new Random(seed)

    override def hasNext: Boolean = {
      n.<(countRecord)
    }

    override def next(): Any = {
      n = n + 1
      generate(schema, random, 0)
    }
  }
}