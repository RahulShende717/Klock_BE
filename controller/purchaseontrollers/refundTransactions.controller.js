import { refundTransactionsData } from "../../businesslogicsprovider/purchaseprovider/refundTransactions.provider.js";

export const refundTransactionsController = async (req, res) => {
  try {
    const transaction_date = req.body.transaction_date;
    const fund = req.body.fund;
    const data = await refundTransactionsData(transaction_date, fund);
    res.status(200).send(data);
  } catch (e) {
    // console.log("Error while fetchinh data for overview", e);
    res.status(500).send({ message: "Internal Server Error" });
  }
};
