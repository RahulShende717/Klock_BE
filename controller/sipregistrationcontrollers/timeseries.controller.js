import { timeseries_provider_sip } from "../../businesslogicsprovider/sipregistrationprovider/timeseries.provider.js";

export const timeseries_controller = async (req, res) => {
  try {
    const { fund, startDate, endDate } = req.body;
    const data = await timeseries_provider_sip(startDate, endDate, fund);
    res.status(200).send(data);
  } catch (e) {
    // console.log("Error in sip timeseris controller", e);
    res.status(500).send({ message: "Internal Server Error" });
  }
};
