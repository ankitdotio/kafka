import { error } from "node:console";
import { kafka } from "../config/kafka";
import { OrderPlaceEvents } from "../events/order-placed.events";

const producer = kafka.producer();

let orderNumber = 1;

function createOrderPlacedEvents(): OrderPlaceEvents {
  const userId = `user - ${Math.ceil(Math.random() * 10)}`;

  return {
    eventId: crypto.randomUUID(),
    event_type: "ORDER_PLACED",
    orderId: `order - ${orderNumber++}`,
    userId,
    amount: Number((Math.random() * 500 + 500).toFixed(2)),
    created_at: new Date().toISOString(),
  };
}

const main = async () => {
  await producer.connect();
  console.log(`ORDER PRODUCER CONNECTED`);
  setInterval(async () => {
    const event = createOrderPlacedEvents();

    await producer.send({
      topic: "ORDER_PLACED",
      messages: [
        {
          key: event.userId,
          value: JSON.stringify(event),
        },
      ],
    });

    console.log(
      `PRODUCED ${event.eventId} : ${event.orderId} for ${event.userId}`,
    );
  }, 2000);
};

process.on("SIGINT", async () => {
  await producer.disconnect();
  process.exit(0);
});

main().catch(async () => {
  console.error(`PRODUCER ERROR : `, error);
  await producer.disconnect();
  process.exit(1);
});
