package datakickstart.kafka.examples

import java.sql.Timestamp
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.Properties

import scala.util.Random
import org.apache.kafka.clients.producer._

object KafkaProducerExample extends App {

  val bootstrapServers = sys.env("KAFKA_BROKERS")
  val mode: String = sys.env("KAFKA_PRODUCER_MODE")

  val props = new Properties()
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  if (mode.toLowerCase == "local") {
    println("Running producer in local mode")
    props.put("bootstrap.servers", "localhost:9092")
  } else {
    println("Running producer in Confluent Cloud mode")
    // If using Confluent
    val kafkaAPIKey = sys.env("KAFKA_API_KEY")
    val kafkaAPISecret = sys.env("KAFKA_API_SECRET")
    props.put("bootstrap.servers", bootstrapServers)
    props.put("security.protocol", "SASL_SSL")
    props.put("sasl.jaas.config", s"org.apache.kafka.common.security.plain.PlainLoginModule  required username='$kafkaAPIKey'   password='$kafkaAPISecret';")
    props.put("sasl.mechanism", "PLAIN")
    // Required for correctness in Apache Kafka clients prior to 2.6
    props.put("client.dns.lookup", "use_all_dns_ips")
    // Best practice for Kafka producer to prevent data loss
    props.put("acks", "all")
  }


  def writeUsageWithPlan(): Unit = {
    val subscriberUsageTopic = "subscriber-usage-demo"

    val producer = new KafkaProducer[String, String](props)

    val randomCompleted = () => { if (Random.nextInt(2) == 1) true else false }
    val randomUser = () => { Random.nextInt(21) }
    val randomDuration= () => { Random.nextInt(360) }
    val timestampNow = () => Timestamp.from(Instant.now)
    val startInstant = Instant.now

    val numRecords = 10000
    val sleepMilliseconds = 0
    val pauseThreshold = 0  // at which record should pause start being enforced, 0 means all records

    val startDate1 = Timestamp.from(startInstant.minus(5, ChronoUnit.MINUTES ))
    val endDate1 = Timestamp.from(startInstant.plus(30, ChronoUnit.DAYS ))
    val endDate10 = Timestamp.from(startInstant.plus(10, ChronoUnit.MINUTES ))
    val timestamp1: Timestamp = timestampNow()
    val timestamp10 = Timestamp.from(Instant.now.plus(2, ChronoUnit.MINUTES))
    val future = Timestamp.from(startInstant.plus(-2, ChronoUnit.DAYS))
    val past = Timestamp.from(startInstant.plus(-1, ChronoUnit.DAYS))

    val memberMap = Map(
      1 -> SubscriptionRecord("a", "user1", "free_plan_1", startDate1, endDate1, timestamp1, timestamp1),
      2 -> SubscriptionRecord("b", "user2", "free_plan_1", startDate1, endDate1, timestamp1, timestamp1),
      3 -> SubscriptionRecord("c", "user3", "paid_plan_1", startDate1, endDate1, timestamp1, timestamp1),
      4 -> SubscriptionRecord("d", "user4", "paid_plan_1", startDate1, endDate1, timestamp1, timestamp1),
      5 -> SubscriptionRecord("e", "user5", "free_plan_2", startDate1, endDate1, timestamp1, timestamp1),
      10 -> SubscriptionRecord("c", "user3", "free_plan_2", startDate1, endDate10, timestamp10, timestamp10),
      11 -> SubscriptionRecord("c", "user3", "paid_plan_2", startDate1, endDate10, timestamp10, timestamp10),
      12 -> SubscriptionRecord("c", "user3", null, startDate1, endDate10, timestamp10, timestamp10)

    )

    def buildSubscriberUsage(i: Int, user: Int, completed: Boolean, duration: Int, timestamp: Timestamp, memberLookup: Int = 0, tombstone: Boolean = false): String = {
      val membershipKey = if(memberLookup == 0) user else memberLookup

      val m: SubscriptionRecord = memberMap(membershipKey)
      if (tombstone) {
//        s"""{"usageId": $i,  "eventTimestamp": "${timestamp}"}"""
        null
      }
      else
      {
        s"""{"usageId": $i, "user": "user${user}", "completed": ${completed}, "durationSeconds": ${duration}, "eventTimestamp": "${timestamp}", "subscriptionId": "${m.subscriptionId}", "plan": "${m.plan}", "startDate": "${m.startDate}", "endDate": "${m.endDate}", "updatedAt":"${m.updatedAt}"}"""
      }
    }

    def produceMemberUsage(usageId: Int, user: Int, pass: Boolean, duration: Int, timestamp: Timestamp, memberLookup: Int = 0, tombstone: Boolean = false) = {
      val mu = buildSubscriberUsage(usageId, user, pass, duration, timestamp, memberLookup, tombstone)
      val record1 = new ProducerRecord[String, String](subscriberUsageTopic, usageId.toString, mu)
      produceRecord(producer, record1)
      Thread.sleep(sleepMilliseconds)
    }

    try {
    //      val ts2 = Timestamp.from(Instant.now.plus(1, ChronoUnit.MINUTES))
        produceMemberUsage(1, 1,  randomCompleted(), 20, timestampNow(), memberLookup=1)
        Thread.sleep(3000)
        produceMemberUsage(2, 2, randomCompleted(), 30, timestampNow(), memberLookup=2)
        Thread.sleep(3000)
        produceMemberUsage(3, 2, randomCompleted(), 15, timestampNow(), memberLookup=2)
        Thread.sleep(3000)
        produceMemberUsage(4, 3, randomCompleted(), 5, timestampNow(), memberLookup=3)

        // Updates (meant to run 1 at a time)
//        produceMemberUsage(4, 3, randomCompleted(), 5, timestampNow(), memberLookup=11)
//        produceMemberUsage(5, 3, randomCompleted(), 25, timestampNow(), memberLookup=11)
//        produceMemberUsage(5, 3, randomCompleted(), 25, timestampNow(), memberLookup=10)


//        produceMemberUsage(7, 3, randomCompleted(), 10, timestampNow(), memberLookup=3)

        // DELETE
        produceMemberUsage(4, 3, randomCompleted(), 5, timestampNow(), memberLookup=11, tombstone=true)

//       produceMemberUsage(6, 3, randomCompleted(), 40, timestampNow(), memberLookup=10)

    }catch{
      case e:Exception => e.printStackTrace()
    }finally {
      producer.close()
    }
  }

  private def produceRecord(producer: KafkaProducer[String, String], record: ProducerRecord[String, String]) = {
    val metadata = producer.send(record)
    printf(s"sent to topic %s: record(key=%s value=%s) " +
      "meta(partition=%d, offset=%d)\n",
      record.topic(), record.key(), record.value(),
      metadata.get().partition(),
      metadata.get().offset()
    )
  }

  private def produceRecord2(producer: KafkaProducer[String, String], record: ProducerRecord[String, String]) = {
    val metadata = producer.send(record)
    printf(s"sent to topic %s: record(key=%s value=%s) " +
      "meta(partition=%d, offset=%d)\n",
      record.topic(), record.key(), record.value(),
      metadata.get().partition(),
      metadata.get().offset()
    )
  }


  writeUsageWithPlan()
}
