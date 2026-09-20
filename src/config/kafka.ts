import { Kafka } from "kafkajs";

export const kafka = new Kafka({
  clientId: "kafka-l1",
  brokers: ["localhost:9092"],
});
