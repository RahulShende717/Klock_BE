import { handle_transaction_histories } from "../../businesslogicsprovider/overviewprovider/trhistory.provider.js";

export const trhistory_controller = async (req, res) => {
  try {
    const transaction_date = req.body.currentDate;
    const fund = req.body.fund;
    const data = await handle_transaction_histories(transaction_date, fund);
    res.status(200).send(data);
  } catch (error) {
    console.error("Error while fetching transaction histories", error);
    res.status(500).send("Internal Server Error");
  }
};
