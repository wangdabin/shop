package untils

import java.util.UUID
import java.nio.ByteBuffer
import java.security.MessageDigest

/*
 * java.util.UUID does not provide a UUID5 implementation,
 * and the apache-commons implementation is incorrect --
 * version 3 and version 5 do NOT have the same bits.
 * This module takes inspiration from apache-commons while
 * at the same time making it work : )
 */
object UUID5 {

  val NAMESPACE_DNS = UUID.fromString("6ba7b810-9dad-11d1-80b4-00c04fd430c8")
  private val UUID_BYTE_LENGTH = 16
  private val SHA1_ENCODING = "SHA-1"

  private def UUIDToBytes(in : UUID) : Array[Byte] = {
    val bb = ByteBuffer.allocate(UUID_BYTE_LENGTH)
    bb.putLong(0, in.getMostSignificantBits)
    bb.putLong(8, in.getLeastSignificantBits)
    bb.array()
  }

  private def bytesToUUID(in : Array[Byte]) : UUID = {
    val bb = ByteBuffer.wrap(in)
    return new UUID(bb.getLong, bb.getLong)
  }

  def apply(name : String, namespace : UUID) : UUID = {
    val concat = UUIDToBytes(namespace) ++ name.getBytes
    val md = MessageDigest.getInstance(SHA1_ENCODING)
    val shaDigest = md.digest(concat)

    shaDigest(6) = (shaDigest(6) & 0x0F).toByte
    shaDigest(6) = (shaDigest(6) | (5 << 4)).toByte
    shaDigest(8) = (shaDigest(8) & 0x3F).toByte
    shaDigest(8) = (shaDigest(8) | 0x80).toByte

    bytesToUUID(shaDigest)
  }

  def fromString(name:String) : String = {
//    val concat = UUIDToBytes(NAMESPACE_DNS) ++ name.getBytes
//    val md = MessageDigest.getInstance(SHA1_ENCODING)
//    val shaDigest = md.digest(concat)
//    shaDigest(6) = (shaDigest(6) & 0x0F).toByte
//    shaDigest(6) = (shaDigest(6) | (5 << 4)).toByte
//    shaDigest(8) = (shaDigest(8) & 0x3F).toByte
//    shaDigest(8) = (shaDigest(8) | 0x80).toByte
//    bytesToUUID(shaDigest).toString
    UUID.nameUUIDFromBytes(name.getBytes()).toString();
  }


  def main(args: Array[String]) {
    println(UUID5.fromString("wd1111b"))
    println(UUID5.fromString("wd1111b"))
  }

}