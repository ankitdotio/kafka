export type OrderPlaceEvents = {
  eventId: string;
  event_type: "ORDER_PLACED";
  orderId: string;
  userId: string;
  amount: number;
  created_at: string;
};

export const ORDER_PLACED_TOPIC = "order.placed";
