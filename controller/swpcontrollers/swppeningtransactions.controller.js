import { pending_txn_swp } from "../../businesslogicsprovider/swpprovider/penidngtrxn.provider.js";

export const pending_trxn_controller = async (req, res) => {
  try {
    const transaction_date = req.body.transaction_date;
    const fund = req.body.fund;
    const data = await pending_txn_swp(transaction_date, fund);
    res.status(200).send(data);
  } catch (e) {
    // console.log("Error while fetchinh data for overview", e);
    res.status(500).send({ message: "Internal Server Error" });
  }
};
