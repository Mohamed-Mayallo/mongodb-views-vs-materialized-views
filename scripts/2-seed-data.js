const { getDb } = require("../src/db");

const CUSTOMERS = Array.from(
  { length: 100 },
  (_, i) => `CUST${String(i + 1).padStart(3, "0")}`
);
const PRODUCTS = Array.from({ length: 50 }, (_, i) => ({
  id: `PROD${String(i + 1).padStart(3, "0")}`,
  name: `Product ${i + 1}`,
  category: ["Electronics", "Clothing", "Books", "Home", "Food"][
    Math.floor(Math.random() * 5)
  ],
  price: Math.floor(Math.random() * 900) + 100,
}));
const STATUS = ["pending", "processing", "shipped", "delivered", "cancelled"];

function generateRandomOrder(orderDate) {
  const customerId = CUSTOMERS[Math.floor(Math.random() * CUSTOMERS.length)];
  const numItems = Math.floor(Math.random() * 5) + 1;
  const items = Array.from({ length: numItems }, () => {
    const product = PRODUCTS[Math.floor(Math.random() * PRODUCTS.length)];
    const quantity = Math.floor(Math.random() * 3) + 1;
    return {
      productId: product.id,
      productName: product.name,
      category: product.category,
      quantity,
      unitPrice: product.price,
      totalPrice: product.price * quantity,
    };
  });

  return {
    orderId: `ORD${Date.now()}${Math.floor(Math.random() * 1000)}`,
    customerId,
    orderDate,
    status: STATUS[Math.floor(Math.random() * STATUS.length)],
    items,
    totalAmount: items.reduce((sum, item) => sum + item.totalPrice, 0),
    shippingAddress: {
      street: `${Math.floor(Math.random() * 999) + 1} Main St`,
      city: ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix"][
        Math.floor(Math.random() * 5)
      ],
      country: "USA",
      zipCode: String(Math.floor(Math.random() * 90000) + 10000),
    },
    paymentMethod: ["credit_card", "debit_card", "paypal", "bank_transfer"][
      Math.floor(Math.random() * 4)
    ],
  };
}

async function seedDatabase() {
  try {
    const db = await getDb();
    const ordersCollection = db.collection(process.env.ORDERS_COLLECTION);

    // Generate orders for the last 36 months
    const orders = [];
    const endDate = new Date();
    const startDate = new Date(endDate);
    startDate.setMonth(startDate.getMonth() - 36);

    for (
      let date = startDate;
      date <= endDate;
      date.setDate(date.getDate() + 1)
    ) {
      const ordersPerDay = Math.floor(Math.random() * 10) + 5; // 5-15 orders per day
      for (let i = 0; i < ordersPerDay; i++) {
        orders.push(generateRandomOrder(new Date(date)));
      }
    }

    // Insert orders in batches
    const batchSize = 1000;
    for (let i = 0; i < orders.length; i += batchSize) {
      const batch = orders.slice(i, i + batchSize);
      await ordersCollection.insertMany(batch);
      console.log(
        `Inserted ${batch.length} orders (${i + batch.length}/${orders.length})`
      );
    }

    console.log("Database seeding completed successfully");
    const totalOrders = await ordersCollection.countDocuments();
    console.log(`Total orders in database: ${totalOrders}`);
  } catch (error) {
    console.error("Error seeding database:", error);
    process.exit(1);
  }
}

seedDatabase().then(() => process.exit(0));
