import { handle_top_pending_transaction } from "../../businesslogicsprovider/overviewprovider/toppendingtransaction.provider.js";

export const top_pending_controller = async (req, res) => {
  try {
    const transaction_date = req.body.currentDate;
    const fund = req.body.fund;
    const data = await handle_top_pending_transaction(transaction_date, fund);
    res.status(200).send(data);
  } catch (e) {
    console.log("Error while fetching top pending transactions", e);
    res.status(500).send({ message: "Internal Server Error" });
  }
};
