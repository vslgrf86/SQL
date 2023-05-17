CREATE MATERIALIZED VIEW bi_mrr._mrr_operational__monthly_changes TABLESPACE pg_default AS WITH daily_mrr AS 
(
   SELECT
      last_day(amd.date) AS month,
      amd.date,
      amd.company_id,
      amd.name,
      amd.pricing_type,
      amd.currency,
      amd.price_per_resource,
      amd.user_billable_count,
      amd.mrr,
      row_number() OVER (PARTITION BY amd.company_id, amd.name, 
      (
         last_day(amd.date)
      )
   ORDER BY
      amd.date DESC, amd.company_id, amd.name) AS rnk 
   FROM
      bi_mrr._mrr_operational__development amd 
   where
      amd.date >= '2022-10-01' 
)
,
monthly_mrr AS 
(
   SELECT
      dm.month,
      dm.date,
      dm.company_id,
      dm.name,
      dm.user_billable_count,
      dm.currency,
      dm.price_per_resource,
      CASE
         WHEN
            dm.date = dm.month 
            OR dm.date = 
            (
               now() - '1 day'::interval
            )
            ::date 
         THEN
            dm.mrr 
         ELSE
            0::double precision 
      END
      AS mrr 
   FROM
      daily_mrr dm 
   WHERE
      dm.rnk = 1 
)
select
   * 
from
   monthly_mrr,
   active_end_of_month AS 
   (
      SELECT
         m.month,
         m.company_id,
         m.name,
         m.user_billable_count,
         m.mrr 
      FROM
         monthly_mrr m 
      WHERE
         m.date = m.month 
         OR m.date = 
         (
            now() - '1 day'::interval
         )
         ::date 
   )
,
   active_end_of_month_companies AS 
   (
      SELECT
         eop.month,
         eop.company_id,
         sum(eop.mrr) AS mrr 
      FROM
         active_end_of_month eop 
      GROUP BY
         eop.month,
         eop.company_id 
   )
,
   first_mrr_companies AS 
   (
      SELECT
         xx.company_id,
         xx.month,
         xx.rnk 
      FROM
         (
            SELECT
               amd.company_id,
               amd.month,
               row_number() OVER (PARTITION BY amd.company_id 
            ORDER BY
               amd.date) AS rnk 
            FROM
               daily_mrr amd 
            WHERE
               amd.mrr <> 0::double precision
         )
         xx 
      WHERE
         xx.rnk = 1 
   )
   SELECT
      mrr.month,
      mrr.date,
      mrr.company_id,
      mrr.name,
      mrr.user_billable_count,
      mrr.currency,
      mrr.price_per_resource,
      mrr.mrr,
	  /*changes' logic start*/
      CASE
         WHEN
            comp_nextm.mrr IS NULL 
         THEN
            'churn'::text 
         WHEN
            comp_lastm.mrr IS NULL 
            AND first_mrr.month = mrr.month 
         THEN
            'new'::text 
         WHEN
            comp_lastm.mrr IS NULL 
         THEN
            'reactivation'::text 
         WHEN
            comp_nextm.mrr < comp_lastm.mrr 
         THEN
            'contraction'::text 
         WHEN
            comp_nextm.mrr > comp_lastm.mrr 
         THEN
            'expansion'::text 
         WHEN
            nextm.mrr IS NULL 
            AND lastm.mrr IS NULL 
         THEN
            'bug'::text 
         ELSE
            'existing'::text 
      END
	  /*changes' logic end*/
      AS event, COALESCE(mrr.mrr, 0::double precision) - COALESCE(lastm.mrr, 0::double precision) AS mrr_delta, COALESCE(mrr.user_billable_count::double precision, 0::double precision) - COALESCE(lastm.user_billable_count::double precision, 0::double precision) AS ubc_mrr_delta 
   FROM
      monthly_mrr mrr 
      LEFT JOIN
         active_end_of_month_companies comp_lastm 
         ON last_day((mrr.month - '1 mon'::interval)::date) = comp_lastm.month 
         AND mrr.company_id = comp_lastm.company_id 
         AND comp_lastm.mrr <> 0::double precision 
      LEFT JOIN
         active_end_of_month_companies comp_nextm 
         ON mrr.month = comp_nextm.month 
         AND mrr.company_id = comp_nextm.company_id 
         AND comp_nextm.mrr <> 0::double precision 
      LEFT JOIN
         first_mrr_companies first_mrr 
         ON first_mrr.company_id = mrr.company_id 
      LEFT JOIN
         active_end_of_month lastm 
         ON last_day((mrr.month - '1 mon'::interval)::date) = lastm.month 
         AND mrr.company_id = lastm.company_id 
         AND mrr.name = lastm.name 
         AND lastm.mrr <> 0::double precision 
      LEFT JOIN
         active_end_of_month nextm 
         ON mrr.month = nextm.month 
         AND mrr.company_id = nextm.company_id 
         AND mrr.name = nextm.name 
         AND nextm.mrr <> 0::double precision 
   WHERE
      mrr.mrr <> 0::double precision 
      OR 
      (
         COALESCE(mrr.mrr, 0::double precision) - COALESCE(lastm.mrr, 0::double precision)
      )
      <> 0::double precision 
   ORDER BY
      mrr.month DESC, mrr.company_id, mrr.name WITH DATA;