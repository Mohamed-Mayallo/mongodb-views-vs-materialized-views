const { getDb } = require("../src/db");
const { performIncrementalUpdate } = require("./4-create-materialized-view");

async function insertOrder() {
  try {
    const db = await getDb();

    // Create a test order with multiple items
    const testOrder = {
      orderId: `ORD${Date.now()}${Math.floor(Math.random() * 1000)}`,
      customerId: "CUST999",
      orderDate: new Date(),
      status: "pending",
      items: [
        {
          productId: "PROD001",
          productName: "Test Product 1",
          category: "Electronics",
          quantity: 2,
          unitPrice: 500,
          totalPrice: 1000,
        },
        {
          productId: "PROD002",
          productName: "Test Product 2",
          category: "Books",
          quantity: 1,
          unitPrice: 30,
          totalPrice: 30,
        },
      ],
      totalAmount: 1030,
      shippingAddress: {
        street: "123 Test Street",
        city: "New York",
        country: "USA",
        zipCode: "10001",
      },
      paymentMethod: "credit_card",
    };

    // Insert the test order
    const result = await db
      .collection(process.env.ORDERS_COLLECTION)
      .insertOne(testOrder);
    console.log(`Inserted test order with ID: ${result.insertedId}`);
    console.log("Order details:", JSON.stringify(testOrder, null, 2));

    // In case of Replica Set (using streams)
    /**
      console.log(
        "The materialized view should be automatically updated via the change stream."
      );
      console.log(
        "Wait a moment and then query the materialized view to verify the update."
      );
     */

    // Refresh the materialized view manually (semi real-time refresh)
    await performIncrementalUpdate(db, result.insertedId);
  } catch (error) {
    console.error("Error inserting test order:", error);
    process.exit(1);
  }
}

// Run the insertion if this script is executed directly
if (require.main === module) {
  insertOrder().then(() => {
    // Keep the process running for a short while to allow the change to reflect the update in the materialized view
    setTimeout(() => process.exit(0), 2000);
  });
}
