SELECT date_part('week'::text, op.datecreate) AS week_num,
    sr.business_region AS region,
    os.shopindex AS store,
    dsn.shop_name_ru AS store_name,
    pt.modelname,
    b.ranking,
    c.check_nums,
    c.avg_num,
    d.total_checks,
    d.avg_num_items,
    count(DISTINCT op.id) AS check_num
   FROM ods_setretail.od_purchase op
     JOIN ods_setretail.od_shift os ON op.id_shift = os.id
     JOIN ods_setretail.od_position pos ON pos.id_purchase = op.id
     JOIN ods_setretail.od_product p ON pos.product_hash = p.hash
     JOIN ods_erp.dict_product pt ON pt.code = p.item
     JOIN history.d_store_new dsn ON dsn.but_num_business_unit = os.shopindex
     LEFT JOIN dev.d_stores_regions_square_meters sr ON sr.store = os.shopindex
     JOIN ( SELECT a.week_num,
            split_part(a.modelname, ' '::text, 1) AS model_num,
            a.num_checks,
            rank() OVER (ORDER BY a.num_checks DESC) AS ranking
           FROM ( SELECT date_part('week'::text, op_1.datecreate) AS week_num,
                    pt_1.modelname,
                    count(DISTINCT op_1.id) AS num_checks
                   FROM ods_setretail.od_purchase op_1
                     JOIN ods_setretail.od_shift os_1 ON op_1.id_shift = os_1.id
                     JOIN ods_setretail.od_position pos_1 ON pos_1.id_purchase = op_1.id
                     JOIN ods_setretail.od_product p_1 ON pos_1.product_hash = p_1.hash
                     JOIN ods_erp.dict_product pt_1 ON pt_1.code = p_1.item
                     JOIN history.d_store_new dsn_1 ON dsn_1.but_num_business_unit = os_1.shopindex
                  WHERE op_1.datecreate > '2024-01-01 00:00:00'::timestamp without time zone AND op_1.checkstatus = 0 AND p_1.item <> '00-00033565'::text AND op_1.datecreate::date < 'now'::text::date AND (p_1.item <> ALL (ARRAY['00-00032263'::text, '00-00032131'::text])) AND date_part('week'::text, op_1.datecreate) = (date_part('week'::text, 'now'::text::date) - 1::double precision) AND (pt_1.dec_producttype <> ALL (ARRAY['НЕГАЗИРОВАННАЯ ВОДА'::text, 'ГАЗИРОВАННАЯ ВОДА'::text, 'ВОДА'::text, 'ПРОТЕИНОВЫЙ СНЕК'::text, 'НАПИТОК'::text, 'ПРОТЕИНОВОЕ ПЕЧЕНЬЕ'::text, 'ПРОТЕИНОВЫЙ БАТОНЧИК'::text, 'ИЗОТОНИК'::text, 'БАТОНЧИК'::text]))
                  GROUP BY date_part('week'::text, op_1.datecreate), pt_1.modelname) a) b ON b.week_num = date_part('week'::text, op.datecreate) AND b.model_num = split_part(pt.modelname, ' '::text, 1) AND b.ranking < 6
     JOIN ( SELECT a.week_num,
            a.store,
            count(a.id) AS check_nums,
            round(sum(a.num) / count(a.id)::numeric, 1) AS avg_num
           FROM ( SELECT date_part('week'::text, op_1.datecreate) AS week_num,
                    sr_1.business_region AS region,
                    os_1.shopindex AS store,
                    dsn_1.shop_name_ru AS store_name,
                    op_1.id,
                    count(DISTINCT p_1.item) AS num
                   FROM ods_setretail.od_purchase op_1
                     JOIN ods_setretail.od_shift os_1 ON op_1.id_shift = os_1.id
                     JOIN ods_setretail.od_position pos_1 ON pos_1.id_purchase = op_1.id
                     JOIN ods_setretail.od_product p_1 ON pos_1.product_hash = p_1.hash
                     JOIN ods_erp.dict_product pt_1 ON pt_1.code = p_1.item
                     JOIN history.d_store_new dsn_1 ON dsn_1.but_num_business_unit = os_1.shopindex
                     LEFT JOIN dev.d_stores_regions_square_meters sr_1 ON sr_1.store = os_1.shopindex
                  WHERE op_1.datecreate > '2024-01-01 00:00:00'::timestamp without time zone AND op_1.checkstatus = 0 AND p_1.item <> '00-00033565'::text AND op_1.datecreate::date < 'today'::text::date AND (p_1.item <> ALL (ARRAY['00-00032263'::text, '00-00032131'::text])) AND date_part('week'::text, op_1.datecreate) = (date_part('week'::text, 'now'::text::date) - 1::double precision) AND (pt_1.dec_producttype <> ALL (ARRAY['НЕГАЗИРОВАННАЯ ВОДА'::text, 'ГАЗИРОВАННАЯ ВОДА'::text, 'ВОДА'::text, 'ПРОТЕИНОВЫЙ СНЕК'::text, 'НАПИТОК'::text, 'ПРОТЕИНОВОЕ ПЕЧЕНЬЕ'::text, 'ПРОТЕИНОВЫЙ БАТОНЧИК'::text, 'ИЗОТОНИК'::text, 'БАТОНЧИК'::text]))
                  GROUP BY date_part('week'::text, op_1.datecreate), sr_1.business_region, os_1.shopindex, dsn_1.shop_name_ru, op_1.id) a
          GROUP BY a.week_num, a.store) c ON c.week_num = date_part('week'::text, op.datecreate) AND c.store = os.shopindex
     JOIN ( SELECT a.week_num,
            count(a.id) AS total_checks,
            round(sum(a.num) / count(a.id)::numeric, 1) AS avg_num_items
           FROM ( SELECT date_part('week'::text, op_1.datecreate) AS week_num,
                    op_1.id,
                    count(DISTINCT p_1.item) AS num
                   FROM ods_setretail.od_purchase op_1
                     JOIN ods_setretail.od_shift os_1 ON op_1.id_shift = os_1.id
                     JOIN ods_setretail.od_position pos_1 ON pos_1.id_purchase = op_1.id
                     JOIN ods_setretail.od_product p_1 ON pos_1.product_hash = p_1.hash
                     JOIN ods_erp.dict_product pt_1 ON pt_1.code = p_1.item
                     JOIN history.d_store_new dsn_1 ON dsn_1.but_num_business_unit = os_1.shopindex
                  WHERE op_1.datecreate > '2024-01-01 00:00:00'::timestamp without time zone AND op_1.checkstatus = 0 AND p_1.item <> '00-00033565'::text AND op_1.datecreate::date < 'now'::text::date AND (p_1.item <> ALL (ARRAY['00-00032263'::text, '00-00032131'::text])) AND (pt_1.dec_producttype <> ALL (ARRAY['НЕГАЗИРОВАННАЯ ВОДА'::text, 'ГАЗИРОВАННАЯ ВОДА'::text, 'ВОДА'::text, 'ПРОТЕИНОВЫЙ СНЕК'::text, 'НАПИТОК'::text, 'ПРОТЕИНОВОЕ ПЕЧЕНЬЕ'::text, 'ПРОТЕИНОВЫЙ БАТОНЧИК'::text, 'ИЗОТОНИК'::text, 'БАТОНЧИК'::text]))
                  GROUP BY date_part('week'::text, op_1.datecreate), op_1.id) a
          GROUP BY a.week_num) d ON d.week_num = date_part('week'::text, op.datecreate)
  WHERE op.datecreate > '2024-01-01 00:00:00'::timestamp without time zone AND op.checkstatus = 0 AND p.item <> '00-00033565'::text AND op.datecreate::date < 'today'::text::date AND (p.item <> ALL (ARRAY['00-00032263'::text, '00-00032131'::text])) AND date_part('week'::text, op.datecreate) = (date_part('week'::text, 'now'::text::date) - 1::double precision) AND (pt.dec_producttype <> ALL (ARRAY['НЕГАЗИРОВАННАЯ ВОДА'::text, 'ГАЗИРОВАННАЯ ВОДА'::text, 'ВОДА'::text, 'ПРОТЕИНОВЫЙ СНЕК'::text, 'НАПИТОК'::text, 'ПРОТЕИНОВОЕ ПЕЧЕНЬЕ'::text, 'ПРОТЕИНОВЫЙ БАТОНЧИК'::text, 'ИЗОТОНИК'::text, 'БАТОНЧИК'::text]))
  GROUP BY date_part('week'::text, op.datecreate), sr.business_region, os.shopindex, dsn.shop_name_ru, pt.modelname, b.ranking, c.check_nums, c.avg_num, d.total_checks, d.avg_num_items;