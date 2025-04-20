const cron = require("node-cron");
const { getDb } = require("../src/db");

async function createMaterializedView() {
  try {
    const db = await getDb();

    // Drop existing materialized view if it exists
    try {
      await db.collection(process.env.MATERIALIZED_VIEW_NAME).drop();
      console.log(
        `Dropped existing materialized view: ${process.env.MATERIALIZED_VIEW_NAME}`
      );
    } catch (error) {
      // Ignore if collection doesn't exist
    }

    // Create the initial materialized view using $out
    await createInitialMaterializedView(db);

    // Set up change stream to track updates (HEADS UP: NEEDS REPLICA SET SETUP)
    // setupChangeStream(db);

    // Schedule periodic full refresh using cron
    setupPeriodicRefresh(db);

    console.log(
      `Created materialized view: ${process.env.MATERIALIZED_VIEW_NAME}`
    );
    console.log(
      "Materialized view creation and update mechanisms completed successfully"
    );
  } catch (error) {
    console.error("Error creating materialized view:", error);
    process.exit(1);
  }
}

async function createInitialMaterializedView(db) {
  const pipeline = [
    // Unwind the items array to analyze individual products
    { $unwind: "$items" },

    // Group by multiple dimensions for comprehensive analysis
    {
      $group: {
        _id: {
          customerId: "$customerId",
          year: { $year: "$orderDate" },
          month: { $month: "$orderDate" },
          category: "$items.category",
          city: "$shippingAddress.city",
        },
        totalOrders: { $sum: 1 },
        totalSpent: { $sum: "$items.totalPrice" },
        avgOrderValue: { $avg: "$totalAmount" },
        totalQuantity: { $sum: "$items.quantity" },
        uniqueProducts: { $addToSet: "$items.productId" },
        paymentMethods: { $addToSet: "$paymentMethod" },
        orderStatuses: { $addToSet: "$status" },
      },
    },

    // Project and calculate additional metrics
    {
      $project: {
        _id: 0,
        customerId: "$_id.customerId",
        year: "$_id.year",
        month: "$_id.month",
        category: "$_id.category",
        city: "$_id.city",
        totalOrders: 1,
        totalSpent: { $round: ["$totalSpent", 2] },
        avgOrderValue: { $round: ["$avgOrderValue", 2] },
        totalQuantity: 1,
        uniqueProductCount: { $size: "$uniqueProducts" },
        paymentMethods: 1,
        orderStatuses: 1,
        avgItemsPerOrder: {
          $round: [{ $divide: ["$totalQuantity", "$totalOrders"] }, 2],
        },
        lastUpdated: new Date(),
      },
    },

    // Sort for better query performance
    {
      $sort: {
        year: -1,
        month: -1,
        totalSpent: -1,
      },
    },

    // Output to a new collection (create the materialized view here)
    { $out: process.env.MATERIALIZED_VIEW_NAME },
  ];

  await db
    .collection(process.env.ORDERS_COLLECTION)
    .aggregate(pipeline)
    .toArray();
  console.log("Initial materialized view created using $out");
}

// HEADS UP: NEEDS REPLICA SET SETUP
function setupChangeStream(db) {
  const changeStream = db.collection(process.env.ORDERS_COLLECTION).watch();

  changeStream.on("change", async (change) => {
    if (
      change.operationType === "insert" ||
      change.operationType === "update" ||
      change.operationType === "delete" ||
      change.operationType === "replace"
    ) {
      // Perform incremental update using $merge
      await performIncrementalUpdate(db, change.documentKey._id);
    }
  });

  console.log("Change stream setup completed");
}

async function performIncrementalUpdate(db, documentId) {
  try {
    const pipeline = [
      // Match the changed document
      { $match: { _id: documentId } },

      // Use the same aggregation logic as initial view
      { $unwind: "$items" },
      {
        $group: {
          _id: {
            customerId: "$customerId",
            year: { $year: "$orderDate" },
            month: { $month: "$orderDate" },
            category: "$items.category",
            city: "$shippingAddress.city",
          },
          totalOrders: { $sum: 1 },
          totalSpent: { $sum: "$items.totalPrice" },
          avgOrderValue: { $avg: "$totalAmount" },
          totalQuantity: { $sum: "$items.quantity" },
          uniqueProducts: { $addToSet: "$items.productId" },
          paymentMethods: { $addToSet: "$paymentMethod" },
          orderStatuses: { $addToSet: "$status" },
        },
      },
      {
        $project: {
          _id: 0,
          customerId: "$_id.customerId",
          year: "$_id.year",
          month: "$_id.month",
          category: "$_id.category",
          city: "$_id.city",
          totalOrders: 1,
          totalSpent: { $round: ["$totalSpent", 2] },
          avgOrderValue: { $round: ["$avgOrderValue", 2] },
          totalQuantity: 1,
          uniqueProductCount: { $size: "$uniqueProducts" },
          paymentMethods: 1,
          orderStatuses: 1,
          avgItemsPerOrder: {
            $round: [{ $divide: ["$totalQuantity", "$totalOrders"] }, 2],
          },
          lastUpdated: new Date(),
        },
      },
      // Merge with existing materialized view
      {
        $merge: {
          into: process.env.MATERIALIZED_VIEW_NAME,
          whenMatched: "replace", // update
          whenNotMatched: "insert", // insert
        },
      },
    ];

    await db
      .collection(process.env.ORDERS_COLLECTION)
      .aggregate(pipeline)
      .toArray();
    console.log("Incremental update completed using $merge");
  } catch (error) {
    console.error("Error performing incremental update:", error);
  }
}

function setupPeriodicRefresh(db) {
  // Schedule full refresh every day at midnight
  cron.schedule("0 0 * * *", async () => {
    try {
      console.log("Starting scheduled full refresh of materialized view");
      await createInitialMaterializedView(db);
      console.log("Scheduled full refresh completed successfully");
    } catch (error) {
      console.error("Error during scheduled refresh:", error);
    }
  });

  console.log("Periodic refresh scheduled");
}

// Only start the materialized view process if this script is run directly
if (require.main === module) {
  createMaterializedView().then(() => process.exit(0));
}

// Export functions for use in the server
module.exports = {
  performIncrementalUpdate,
};
