import { switch_transction_provider } from "../../businesslogicsprovider/switchprovider/switchtransaction.provider.js";

export const transaction_history_controller = async (req, res) => {
  try {
    const { fund, transaction_date } = req.body;
    const data = await switch_transction_provider(transaction_date, fund);
    res.status(200).send(data);
  } catch (e) {
    // console.log("Error in purchase timeseris controller", e);
    res.status(500).send({ message: "Internal Server Error" });
  }
};
