import e, { response } from "express";
import moment from "moment";
import OverViewMaster from "../models/overview.model.js";
import DataLoadLogs from "../models/dataloadlogs.js";
import { getTableNames } from "../utils/tratablenamemapping.js";
import OverViewMasterNew from "../models/newoverview.model.js";
import CachedData from "../models/cacheddata.model.js";
import { getFundWiseSchema } from "../utils/schemamapping.js";
import { getPoolObj } from "../utils/pool.js";

const handle_cached_calender_data = async (fund, trtype) => {
  try {
    let cdate = moment().format("YYYY-MM-DD");
    const keysToCheck = ["calendarData", "holidayData"];
    let schema = getFundWiseSchema(fund);
    const pool = getPoolObj(fund);
    if (pool == null) console.log("No Pool Connections For This AMC", fund);
    let datesStatuscached = await CachedData.findOne({
      $and: [
        { fund: fund, date: cdate, type: `${trtype}-calendar` },
        {
          $or: keysToCheck.map((key) => ({
            [`dataObject.${key}`]: { $exists: true },
          })),
        },
      ],
    });

    if (datesStatuscached != null) {
      return datesStatuscached;
    } else {
      let prod_query = `select
      trdt,
        (
          case
            when (DATE_PART(
              'day',
              nullif(cast(current_date as text), '')::timestamp - cast(trdt as text)::timestamp
            ))-count_of_h <= 2 then 'within TAT' else 'beyond TAT'
          end
        )
      from (select distinct substring(trdt, 1, 10) as trdt,
      count(distinct(hm_hdate)) as count_of_h
      from schema.tablename a
      left join schema.holiday_master_mirror b on a.fund = b.hm_fund and a.scheme = b.hm_scheme
      and a.plan = b.hm_plan and cast(hm_hdate as date) between cast(trdt as date) and cast(current_date as date)
      and hm_hdate >= '2024-04-01' and hm_hdate <= cast(current_date as text) 
      where substring(trdt, 1, 10) <= cast(current_date as text) and (batchclosedt is null or batchclosedt ='') and active_flag = 'P' ${
        trtype === "sip" ? `and trtype = 'SIN'` : ""
      }
      group by 1
                        order by 1 desc) x`;

      if (trtype == "sip_registration") {
        prod_query = `select
      trdt,
        (
          case
            when (DATE_PART(
              'day',
              nullif(cast(current_date as text), '')::timestamp - cast(trdt as text)::timestamp
            ))-count_of_h <= 2 then 'within TAT' else 'beyond TAT'
          end
        )
      from (select distinct substring(sip_regdt, 1, 10) as trdt,
      count(distinct(hm_hdate)) as count_of_h
      from schema.tablename a
      left join schema.holiday_master_mirror b on a.sip_fund = b.hm_fund and a.sip_scheme = b.hm_scheme
      and a.sip_plan = b.hm_plan and cast(hm_hdate as date) between cast(sip_regdt as date) and cast(current_date as date)
      and hm_hdate >= '2024-04-01' and hm_hdate <= cast(current_date as text) 
      where substring(sip_regdt, 1, 10) <= cast(current_date as text) 
			group by 1
                        order by 1 desc) x`;
      }

      let prod_query1 = `select distinct cast(substring(hm_hdate, 1, 10) as date),
	(
		case
			when concat(
				'IT IS A NON BUSINESS DAY FOR ',
				string_agg(fm_category::text, ',')
			) like '%CASH FUND,DEBT FUND,EQUITY FUND,INCOME FUND,LIQUID FUND%' then concat(
				'IT IS A NON BUSINESS DAY (',
				hm_reason,
				') FOR ALL'
			) else concat(
				'IT IS A NON BUSINESS DAY (',
				hm_reason,
				') FOR ',
				string_agg(fm_category::text, ',')
			)
		end
	) as msg
from (
		select distinct hm_fund,
			hm_hdate,
			hm_reason,
			fm_category
		from schema.holiday_master_mirror a
			inner join schema.fund_master_mirror b on a.hm_fund = b.fm_fund
			and a.hm_scheme = b.fm_scheme
			and a.hm_plan = b.fm_plan
		where hm_hdate >= '2024-04-01'
			and hm_hdate <= '2025-04-01' 
			--and cast(substring(hm_hdate,1,10) as date) = '2024-05-04'
		order by hm_fund,
			hm_hdate,
			hm_reason,
			fm_category
	) a
group by hm_fund,
	hm_hdate,
	hm_reason
order by 1`;

      let uat_query1 = `select distinct cast(substring(hm_hdate,1,10) as date),
      (case
      when concat('IT IS A NON BUSINESS DAY FOR ',string_agg(fm_category::text,',')) like '%CASH FUND,DEBT FUND,EQUITY FUND,INCOME FUND,LIQUID FUND%'
      then 'IT IS A NON BUSINESS DAY FOR ALL'
      else concat('IT IS A NON BUSINESS DAY FOR ',string_agg(fm_category::text,','))
      end) as msg
      from
      (select distinct
      hm_fund,
      hm_hdate,
      fm_category
      from dbo.holiday_master_partition a
      inner join dbo.fund_master_partition b on a.hm_fund = b.fm_fund and a.hm_scheme = b.fm_scheme and a.hm_plan = b.fm_plan
      where hm_hdate >= '2023-04-01' and hm_hdate <= '2026-04-01'
      --and cast(substring(hm_hdate,1,10) as date) = '2024-05-04'
      order by hm_fund,hm_hdate,fm_category) a
            group by hm_fund, hm_hdate`;

      if (trtype === "purchase") {
        const query = `select
      trdt,
        (
          case
            when (DATE_PART(
              'day',
              nullif(cast(current_date as text), '')::timestamp - cast(trdt as text)::timestamp
            ))-count_of_h <= 2 then 'within TAT' else 'beyond TAT'
          end
        )
      from (select distinct substring(trdt, 1, 10) as trdt,
      count(distinct(hm_hdate)) as count_of_h
      from schema.final_non_batchclosed a
      left join schema.holiday_master_mirror b on a.fund = b.hm_fund and a.scheme = b.hm_scheme
      and a.plan = b.hm_plan and cast(hm_hdate as date) between cast(trdt as date) and cast(current_date as date)
      and hm_hdate >= '2024-04-01' and hm_hdate <= cast(current_date as text)
      where substring(trdt, 1, 10) <= cast(current_date as text) and (batchclosedt is null or batchclosedt ='') 
			and active_flag = 'P'
		and trtype in ('ADD','NEW','IPO')
      group by 1
                        order by 1 desc) x`;

        let updatedQuery = query.replace(/schema/g, `${schema}`);

        const calendarData = await pool.query(updatedQuery);
        let updatedQuery1 = prod_query1.replace(/schema/g, `${schema}`);
        const holidayData = await pool.query(updatedQuery1);

        let cachedData = await CachedData.findOneAndUpdate(
          { fund: fund, type: `${trtype}-calendar` },
          {
            $set: {
              date: cdate,
              "dataObject.calendarData": calendarData.rows,
              "dataObject.holidayData": holidayData.rows,
            },
          },
          {
            upsert: true,
            new: true,
          }
        );

        return cachedData;
      } else if (trtype === "sip_registration") {
        const query = `select trdt, ( case when (DATE_PART( 'day', nullif(cast(current_date as text), '')::timestamp - cast(trdt as text)::timestamp ))-count_of_h <= 15 then 'within TAT' else 'beyond TAT' end ) from (select distinct substring(sip_regdt, 1, 10) as trdt, count(distinct(hm_hdate)) as count_of_h from schema.sipregistrationnonfinal a left join schema.holiday_master_mirror b on a.sip_fund = b.hm_fund and a.sip_scheme = b.hm_scheme and a.sip_plan = b.hm_plan and cast(hm_hdate as date) between cast(sip_regdt as date) and cast(current_date as date) and hm_hdate >= '2024-04-01' and hm_hdate <= cast(current_date as text) where substring(sip_regdt, 1, 10) <= cast(current_date as text) 			group by 1 order by 1 desc) x`;

        let updatedQuery = query.replace(/schema/g, `${schema}`);
        const calendarData = await pool.query(updatedQuery);
        let updatedQuery1 = prod_query1.replace(/schema/g, `${schema}`);
        const holidayData = await pool.query(updatedQuery1);

        let cachedData = await CachedData.findOneAndUpdate(
          { fund: fund, type: `${trtype}-calendar` },
          {
            $set: {
              date: cdate,
              "dataObject.calendarData": calendarData.rows,
              "dataObject.holidayData": holidayData.rows,
            },
          },
          {
            upsert: true,
            new: true,
          }
        );

        return cachedData;
      } else if (trtype === "sip_transaction") {
        const query = `select
      trdt,
        (
          case
            when (DATE_PART(
              'day',
              nullif(cast(current_date as text), '')::timestamp - cast(trdt as text)::timestamp
            ))-count_of_h <= 2 then 'within TAT' else 'beyond TAT'
          end
        )
      from (select distinct substring(trdt, 1, 10) as trdt,
      count(distinct(hm_hdate)) as count_of_h
      from schema.final_non_batchclosed a
      left join schema.holiday_master_mirror b on a.fund = b.hm_fund and a.scheme = b.hm_scheme
      and a.plan = b.hm_plan and cast(hm_hdate as date) between cast(trdt as date) and cast(current_date as date)
      and hm_hdate >= '2024-04-01' and hm_hdate <= cast(current_date as text)
      where substring(trdt, 1, 10) <= cast(current_date as text) and (batchclosedt is null or batchclosedt ='') 
			and active_flag = 'P'
		and trtype in ('SIN')
      group by 1
                        order by 1 desc) x`;

        let updatedQuery = query.replace(/schema/g, `${schema}`);
        const calendarData = await pool.query(updatedQuery);
        let updatedQuery1 = prod_query1.replace(/schema/g, `${schema}`);
        const holidayData = await pool.query(updatedQuery1);

        let cachedData = await CachedData.findOneAndUpdate(
          { fund: fund, type: `${trtype}-calendar` },
          {
            $set: {
              date: cdate,
              "dataObject.calendarData": calendarData.rows,
              "dataObject.holidayData": holidayData.rows,
            },
          },
          {
            upsert: true,
            new: true,
          }
        );

        return cachedData;
      } else {
        const tablename = getTableNames(trtype);

        let execution_query = prod_query;
        let execution_query1 = prod_query1;

        let updatedQuery = execution_query.replace(/tablename/g, tablename);
        updatedQuery = updatedQuery.replace(/schema/g, `${schema}`);

        let updatedQuery1 = execution_query1.replace(/schema/g, `${schema}`);
        const calendarData = await pool.query(updatedQuery);
        const holidayData = await pool.query(updatedQuery1);

        let cachedData = await CachedData.findOneAndUpdate(
          { fund: fund, type: `${trtype}-calendar` },
          {
            $set: {
              date: cdate,
              "dataObject.calendarData": calendarData.rows,
              "dataObject.holidayData": holidayData.rows,
            },
          },
          {
            upsert: true,
            new: true,
          }
        );

        return cachedData;
      }
    }
  } catch (error) {
    console.log(error);
    throw error;
  }
};

const getDatesStatus = async (req, res) => {
  try {
    let fund = req.body.fund;
    let trtype = req.body.trtype;
    let data = await handle_cached_calender_data(fund, trtype);

    res.status(200).send({
      success: true,
      response: data.dataObject.calendarData,
      holiday: data.dataObject.holidayData,
    });
  } catch (e) {
    res.status(504).send({ success: false, message: e.message });
  }
};

const lastUpdated = async (req, res) => {
  try {
    const fund = req.body.fund;
    const lastUpdated = await DataLoadLogs.findOne({ fund: fund })
      .sort({ createdAt: -1 })
      .lean()
      .exec();
    res.status(200).send(lastUpdated);
  } catch (err) {
    console.log("err", err);
    res.status(500).send({ message: "Internal Server Error" });
  }
};

export { getDatesStatus, lastUpdated, handle_cached_calender_data };
