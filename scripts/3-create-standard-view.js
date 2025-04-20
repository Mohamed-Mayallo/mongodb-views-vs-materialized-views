const { getDb } = require("../src/db");

async function createStandardView() {
  try {
    const db = await getDb();

    // Drop existing view if it exists
    try {
      await db.collection(process.env.STANDARD_VIEW_NAME).drop();
      console.log(`Dropped existing view: ${process.env.STANDARD_VIEW_NAME}`);
    } catch (error) {
      // Ignore if view doesn't exist
    }

    // Create a complex standard view with rich analytics
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
          // Customer metrics
          totalOrders: { $sum: 1 },
          totalSpent: { $sum: "$items.totalPrice" },
          avgOrderValue: { $avg: "$totalAmount" },
          // Product metrics
          totalQuantity: { $sum: "$items.quantity" },
          uniqueProducts: { $addToSet: "$items.productId" },
          // Payment metrics
          paymentMethods: { $addToSet: "$paymentMethod" },
          // Status metrics
          orderStatuses: { $addToSet: "$status" },
        },
      },

      // Calculate additional metrics
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
          // Derived metrics
          avgItemsPerOrder: {
            $round: [{ $divide: ["$totalQuantity", "$totalOrders"] }, 2],
          },
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
    ];

    // Create the view
    await db.createCollection(process.env.STANDARD_VIEW_NAME, {
      viewOn: process.env.ORDERS_COLLECTION,
      pipeline,
    });

    console.log(`Created standard view: ${process.env.STANDARD_VIEW_NAME}`);
    console.log("Standard view creation completed successfully");
  } catch (error) {
    console.error("Error creating standard view:", error);
    process.exit(1);
  }
}

createStandardView().then(() => process.exit(0));
