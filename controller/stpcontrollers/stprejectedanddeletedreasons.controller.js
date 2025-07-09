import { rejectedDeletedReasons } from "../../businesslogicsprovider/stpprovider/stprejectedanddeletedreasons.provider.js";

export const rejected_deleted_reasons = async (req, res) => {
  try {
    const transaction_date = req.body.transaction_date;
    const fund = req.body.fund;
    const data = await rejectedDeletedReasons(transaction_date, fund);
    res.status(200).send(data);
  } catch (e) {
    // console.log("Error in pending transaction controller purchase", e);
    res.status(500).send({ message: "Internal Server Error" });
  }
};
