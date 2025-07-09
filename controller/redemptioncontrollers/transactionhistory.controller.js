import { transaction_history_provider } from "../../businesslogicsprovider/redemptionprovider/transactionhistory.provider.js";

export const transaction_history_controller = async (req, res) => {
  try {
    let dataObject = {};
    let transaction_date = req.body.transaction_date;
    let fund = req.body.fund;
    const result = await transaction_history_provider(transaction_date, fund);
    res.status(200).send(result);
  } catch (e) {
    // console.log("Error while fetchinh data for overview", e);
    res.status(500).send({ message: "Internal Server Error" });
  }
};
