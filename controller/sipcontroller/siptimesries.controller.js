import { SIPTimeSeriesData } from "../../businesslogicsprovider/sipprovider/siptimeseries.provider.js";
// import { timeseries_provider } from "../../businesslogicsprovider/sipregistrationprovider/timeseries.provider.js";

export const timeseries_controller = async (req, res) => {
  try {
    const { fund, startDate, endDate } = req.body;
    const data = await SIPTimeSeriesData(startDate, endDate, fund);
    res.status(200).send(data);
  } catch (e) {
    // console.log("Error in sip timeseris controller", e);
    res.status(500).send({ message: "Internal Server Error" });
  }
};
