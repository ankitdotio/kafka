import { Kafka } from "kafkajs";
import { kafka } from "./kafka";
import { ORDER_PLACED_TOPIC } from "../events/order-placed.events";
import { error } from "node:console";

const admin = kafka.admin();

const main = async () => {
  await admin.connect();
  await admin.createTopics({
    topics: [
      {
        topic: "ORDER_PLACED_TOPIC",
        numPartitions: 3,
        replicationFactor: 1,
      },
    ],
  });
  console.log(`TOPIC CREATED ${ORDER_PLACED_TOPIC}`);
  await admin.disconnect();
};

main().catch(async (error) => {
  console.log(`TOPIC CREATION ERROR`);
  await admin.disconnect();
  process.exit(1);
});
