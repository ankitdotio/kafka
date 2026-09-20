import { kafka } from "../config/kafka";
import { OrderPlaceEvents } from "../events/order-placed.events";

const consumer = kafka.consumer({ groupId: "analytics-group" });

const main = async () => {
  await consumer.connect();
  console.log("ANALYTICS CONSUMER CONNECTED");

  consumer.subscribe({
    topic: "ORDER_PLACED",
    fromBeginning: true,
  });

  await consumer.run({
    eachMessage: async ({ partition, message }) => {
      const event = JSON.parse(
        message.value?.toString() ?? "{}",
      ) as OrderPlaceEvents;

      console.log(
        `ANALYTICS | PARTITION =${partition} OFFSET = ${message.offset}`,
      );
      console.log(
        `TRACK REVENUE ${event.amount} FROM THIS ORDER ${event.orderId}`,
      );
    },
  });
};

process.on("SIGINT", async () => {
  await consumer.disconnect();
  process.exit(0);
});

main().catch(async (error) => {
  console.error(`PROD UCER ERROR : `, error);
  await consumer.disconnect();
  process.exit(1);
});
