package datakickstart.kafka

import java.sql.Timestamp

package object examples {

  case class UsageEvent(usageId: Int, user: String, completed: Boolean, durationSeconds: Int, eventTimestamp: Timestamp)
  case class SubscriptionRecord(subscriptionId: String, user: String, plan: String, startDate: Timestamp, endDate: Timestamp, updatedAt: Timestamp, eventTimestamp: Timestamp)
}


// topic names: genericUsage
// schema:
/*
{
  "fields": [
    {
      "name": "usageId",
      "type": "string"
    },
    {
      "name": "userHandle",
      "type": "string"
    },
    {
      "name": "eventTime",
      "type": {
        "logicalType": "iso-datetime",
        "type": "string"
      }
    }
  ],
  "name": "genericUsage",
  "namespace": "test",
  "type": "record"
}

 */