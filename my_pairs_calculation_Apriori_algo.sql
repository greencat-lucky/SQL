----------------------------

SELECT a.analysis_period, ----season_to_last_week
    a.model1,
    a.model2,
    a.pair_count,
    a.num_checks
   FROM ( SELECT 'season_to_last_week'::text AS analysis_period,
            t1.modelname AS model1,
            t2.modelname AS model2,
            count(*) AS pair_count,
            ( SELECT count(DISTINCT fso.check_id) AS count
                   FROM dev.f_sales_2024_os fso
                  WHERE fso.date_2024 >= '2024-09-01'::date AND date_part('week'::text, fso.date_2024) < date_part('week'::text, now())) AS num_checks
           FROM ( SELECT fso.check_id,
                    fso.modelname
                   FROM dev.f_sales_2024_os fso
                  WHERE fso.date_2024 >= '2024-09-01'::date AND date_part('week'::text, fso.date_2024) < date_part('week'::text, now())
                  GROUP BY fso.check_id, fso.modelname) t1
             JOIN ( SELECT fso.check_id,
                    fso.modelname
                   FROM dev.f_sales_2024_os fso
                  WHERE fso.date_2024 >= '2024-09-01'::date AND date_part('week'::text, fso.date_2024) < date_part('week'::text, now())
                  GROUP BY fso.check_id, fso.modelname) t2 ON t1.check_id = t2.check_id AND t1.modelname < t2.modelname
          GROUP BY 'season_to_last_week'::text, t1.modelname, t2.modelname
          ORDER BY count(*) DESC
         LIMIT 30) a
UNION ALL
 SELECT b.analysis_period,  ---last week
    b.model1,
    b.model2,
    b.pair_count,
    b.num_checks
   FROM ( SELECT 'last_week'::text AS analysis_period,
            t4.modelname AS model1,
            t4_2.modelname AS model2,
            count(*) AS pair_count,
            ( SELECT count(DISTINCT fso.check_id) AS count
                   FROM dev.f_sales_2024_os fso
                  WHERE date_part('week'::text, fso.date_2024) = (date_part('week'::text, now()) - 1::double precision) AND date_part('year'::text, fso.date_2024) = 2025::double precision) AS num_checks
           FROM ( SELECT fso.check_id,
                    fso.modelname
                   FROM dev.f_sales_2024_os fso
                  WHERE date_part('week'::text, fso.date_2024) = (date_part('week'::text, now()) - 1::double precision) AND date_part('year'::text, fso.date_2024) = 2025::double precision
                  GROUP BY fso.check_id, fso.modelname) t4
             JOIN ( SELECT fso.check_id,
                    fso.modelname
                   FROM dev.f_sales_2024_os fso
                  WHERE date_part('week'::text, fso.date_2024) = (date_part('week'::text, now()) - 1::double precision) AND date_part('year'::text, fso.date_2024) = 2025::double precision
                  GROUP BY fso.check_id, fso.modelname) t4_2 ON t4.check_id = t4_2.check_id AND t4.modelname < t4_2.modelname
          GROUP BY 'last_week'::text, t4.modelname, t4_2.modelname
          ORDER BY count(*) DESC
         LIMIT 30) b;
