WITH year_2024 AS (
                 SELECT op.datecreate::date AS date_2024,
                    d.day_id_day_comp AS date_2021_comp,
                    os.shopindex AS store_num,
                    pt_1.sector_name AS sector,
                    pt_1.family_num::integer AS family_num,
                    round(sum((
                        CASE
                            WHEN op.operationtype THEN 1
                            ELSE '-1'::integer
                        END * pos.sumfield)::numeric / 100::numeric), 0) AS to_2024,
                    sum((
                        CASE
                            WHEN op.operationtype THEN 1
                            ELSE '-1'::integer
                        END * pos.qnty)::numeric / 1000::numeric) AS qty_2024
                   FROM ods_setretail.od_purchase op
                     LEFT JOIN ods_setretail.od_shift os ON op.id_shift = os.id
                     LEFT JOIN ods_setretail.od_position pos ON pos.id_purchase = op.id
                     LEFT JOIN ods_setretail.od_product p ON pos.product_hash = p.hash
                     LEFT JOIN cds.d_product pt_1 ON pt_1.erpcode = p.item
                     JOIN history.d_day_new d ON d.day_id_day = op.datecreate::date
                  WHERE 1 = 1 AND op.datecreate > '2024-01-01 00:00:00'::timestamp without time zone AND op.checkstatus = 0 AND op.datecreate::date < now()::date AND (pt_1.sector_name <> ALL (ARRAY['89 - FURNITURE & FIXTURE'::character varying::text, '90 - SERVICES'::character varying::text, '9 - PRODUCTION'::text, '391 - OTHER SERVICES WORKSHOP'::character varying::text]))
                  GROUP BY op.datecreate::date, d.day_id_day_comp, os.shopindex, pt_1.sector_name, pt_1.family_num::integer
                UNION ALL
                 SELECT t.date AS date_2024,
                    d.day_id_day_comp AS date_2021_comp,
                        CASE
                            WHEN t.channel = 'ECOM'::text THEN 1
                            WHEN t.channel = 'Marketplace'::text THEN 2
                            WHEN t.channel = 'B2B'::text THEN 3
                            ELSE 0
                        END AS store_num,
                    t.sector,
                    "substring"(t.family, '^(\d*)'::text)::integer AS family_num,
                    sum(t."to") AS to_2024,
                    sum(t.qty) AS qty_2024
                   FROM dev.sales_stores t
                     JOIN history.d_day_new d ON d.day_id_day = t.date
                  WHERE 1 = 1 AND (t.channel = ANY (ARRAY['ECOM'::text, 'Marketplace'::text, 'B2B'::text])) AND t.date >= '2024-01-01'::date AND t.date < now()::date AND (t.sector <> ALL (ARRAY['89 - FURNITURE & FIXTURE'::character varying::text, '90 - SERVICES'::character varying::text, '9 - PRODUCTION'::text, '391 - OTHER SERVICES WORKSHOP'::character varying::text]))
                  GROUP BY t.date, d.day_id_day_comp,
                        CASE
                            WHEN t.channel = 'ECOM'::text THEN 1
                            WHEN t.channel = 'Marketplace'::text THEN 2
                            WHEN t.channel = 'B2B'::text THEN 3
                            ELSE 0
                        END, t.sector, "substring"(t.family, '^(\d*)'::text)::integer
                ), year_2021 AS (
                 SELECT ts.date_2021,
                    ts.date_2024_comp,
                    ts.store_num,
                    ts.sector,
                    ts.new_family_num,
                    sum(ts.to_sum) AS to_2021,
                    sum(ts.qty_sum) AS qty_2021
                   FROM dev.f_sales_offline_2021_new_tree_os ts
                     JOIN ( SELECT os_1.shopindex AS store_num
                           FROM ods_setretail.od_purchase op_1
                             JOIN ods_setretail.od_shift os_1 ON op_1.id_shift = os_1.id
                             JOIN ods_setretail.od_position pos_1 ON pos_1.id_purchase = op_1.id
                          WHERE op_1.datecreate > '2024-01-01 00:00:00'::timestamp without time zone AND op_1.datecreate::date < now()::date
                          GROUP BY os_1.shopindex) open_stores_1 ON open_stores_1.store_num = ts.store_num
                  WHERE ts.date_2021 < (( SELECT ddn.day_id_day_comp
                           FROM history.d_day_new ddn
                          WHERE ddn.day_id_day = now()::date)) AND ts.date_2021 >= '2021-01-04'::date
                  GROUP BY ts.date_2021, ts.date_2024_comp, ts.store_num, ts.sector, ts.new_family_num
                UNION ALL
                 SELECT ts.date_2021,
                    ts.date_2024_comp,
                    ts.store_num,
                    ts.sector,
                    ts.new_family_num,
                    sum(ts.to_sum) AS to_2021,
                    sum(ts.qty_sum) AS qty_2021
                   FROM dev.f_sales_offline_2021_new_tree_os ts
                  WHERE ts.date_2021 < (( SELECT ddn.day_id_day_comp
                           FROM history.d_day_new ddn
                          WHERE ddn.day_id_day = now()::date)) AND ts.date_2021 >= '2021-01-04'::date AND (ts.store_num = ANY (ARRAY[1::bigint, 2::bigint, 3::bigint]))
                  GROUP BY ts.date_2021, ts.date_2024_comp, ts.store_num, ts.sector, ts.new_family_num
                )
         SELECT year_2024.date_2024,
            year_2021.date_2024_comp,
            year_2021.date_2021,
            year_2024.date_2021_comp,
            year_2024.store_num AS store_2024,
            year_2021.store_num AS store_2021,
            year_2024.sector AS sector_2024,
            year_2021.sector AS sector_2021,
            year_2024.family_num,
            year_2021.new_family_num,
            year_2024.to_2024,
            year_2021.to_2021,
            year_2024.qty_2024,
            year_2021.qty_2021
           FROM year_2024
             FULL JOIN year_2021 ON year_2024.date_2024 = year_2021.date_2024_comp AND year_2024.store_num = year_2021.store_num AND year_2024.family_num = year_2021.new_family_num
        )
 SELECT dsm.macro_level_scm AS macro_sector,
    COALESCE(a.sector_2024, a.sector_2021::text) AS sector,
    COALESCE(a.family_num, a.new_family_num) AS fam_num,
    nt.new_family_name::text AS family_name,
    COALESCE(a.date_2024, a.date_2024_comp) AS date_2024,
    COALESCE(a.date_2021, a.date_2021_comp) AS date_2021,
    COALESCE(a.store_2021, a.store_2024) AS store,
        CASE
            WHEN COALESCE(a.store_2021, a.store_2024) = 1 THEN 'ECOM'::text
            WHEN COALESCE(a.store_2021, a.store_2024) = 2 THEN 'Marketplace'::text
            WHEN COALESCE(a.store_2021, a.store_2024) = 3 THEN 'B2B'::text
            ELSE dsn.shop_name_ru
        END AS store_name,
        CASE
            WHEN COALESCE(comparable.store_num, 0::bigint) = 0 THEN 'closed'::text
            ELSE 'comparable'::text
        END AS store_type,
    COALESCE(a.to_2024, 0::numeric::double precision) AS to_2024,
    COALESCE(a.to_2021, 0::numeric::double precision) AS to_2021,
    COALESCE(a.qty_2024, 0::numeric::double precision) AS qty_2024,
    COALESCE(a.qty_2021, 0::bigint::numeric::double precision) AS qty_2021
   FROM a
     LEFT JOIN dev.d_old_new_family_match nt ON nt.new_fam_num = COALESCE(a.family_num, a.new_family_num)
     LEFT JOIN cds.d_store_new dsn ON dsn.but_num_business_unit = COALESCE(a.store_2021, a.store_2024)
     LEFT JOIN dev.d_sport_macro dsm ON dsm.sector::text = COALESCE(a.sector_2024, a.sector_2021::text)
     LEFT JOIN ( SELECT t.store_num
           FROM dev.f_sales_offline_2021_new_tree_os t
          WHERE t.date_2021 < (( SELECT ddn.day_id_day_comp
                   FROM history.d_day_new ddn
                  WHERE ddn.day_id_day = now()::date))
          GROUP BY t.store_num) comparable ON comparable.store_num = COALESCE(a.store_2021, a.store_2024)
  GROUP BY dsm.macro_level_scm, COALESCE(a.sector_2024, a.sector_2021::text), COALESCE(a.family_num, a.new_family_num), nt.new_family_name::text, COALESCE(a.date_2024, a.date_2024_comp), COALESCE(a.date_2021, a.date_2021_comp), COALESCE(a.store_2021, a.store_2024), dsn.shop_name_ru,
        CASE
            WHEN COALESCE(comparable.store_num, 0::bigint) = 0 THEN 'closed'::text
            ELSE 'comparable'::text
        END, COALESCE(a.to_2024, 0::numeric::double precision), COALESCE(a.to_2021, 0::numeric::double precision), COALESCE(a.qty_2024, 0::numeric::double precision), COALESCE(a.qty_2021, 0::bigint::numeric::double precision)