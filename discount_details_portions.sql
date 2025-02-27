WITH main_sales AS (
         SELECT op.id,
            op.datecommit,
            op.datecreate,
            os.cashnum,
            op.id_session,
            op.numberfield AS opnumfield,
            os.numshift,
                CASE
                    WHEN op.operationtype THEN 1
                    ELSE '-1'::integer
                END * opos.qnty / 1000 AS qnty,
            opos.numberfield,
                CASE
                    WHEN op.operationtype THEN 1
                    ELSE '-1'::integer
                END::numeric * opos.sumfield::numeric * 1.0 / 100::numeric AS sumfield,
                CASE
                    WHEN op.operationtype THEN 1
                    ELSE '-1'::integer
                END::numeric * opos.sumdiscount::numeric * 1.0 / 100::numeric AS main_discount,
            dsn.shop_name_ru,
            dsn.but_num_business_unit,
            cdspr.modelcode,
            cdspr.skucode,
            cdspr.modelname,
            cdspr.dec_producttype,
            pr.item,
            opc.card_number
           FROM ods_setretail.od_purchase op
             JOIN ods_setretail.od_shift os ON op.id_shift = os.id
             JOIN ods_setretail.od_position opos ON opos.id_purchase = op.id
             JOIN ods_setretail.od_product pr ON opos.product_hash = pr.hash
             LEFT JOIN cds.d_product cdspr ON cdspr.erpcode = pr.item
             JOIN cds.d_store_new dsn ON os.shopindex = dsn.but_num_business_unit AND op.datecommit >= dsn.desport_opened
             LEFT JOIN ods_setretail.od_purchase_cards opc ON opc.purchase = op.id AND opc.card_type = 'ExternalCard'::text
          WHERE op.checkstatus = 0 AND pr.item <> '00-00033565'::text AND (pr.item <> ALL (ARRAY['00-00032263'::text, '00-00032131'::text]))
        ), discount AS (
         SELECT lt.cash_number,
            lt.sale_time,
            lt.shop_number,
            lt.purchase_number,
            lt.transaction_time,
            lt.shift_number,
            ldp.position_order,
                CASE
                    WHEN lt.operation_type THEN 1
                    ELSE '-1'::integer
                END * ldp.qnty / 1000 AS qnty,
            ldp.discount_type,
                CASE
                    WHEN lt.operation_type THEN 1
                    ELSE '-1'::integer
                END::numeric * ldp.discount_amount::numeric * 1.0 / 100::numeric AS discount_amount,
            ldp.good_code,
            ldp.discount_identifier,
            ldp.discount_name
           FROM ods_setretail.loy_transaction lt
             JOIN ods_setretail.loy_discount_positions ldp ON ldp.transaction_id = lt.id
          WHERE lt.status = 0
        )
 SELECT ms.datecommit::date AS day_id,
    ms.but_num_business_unit AS store,
    ms.id AS check_id,
    ms.numberfield,
    ms.skucode,
    ms.qnty AS sales_qty,
    ms.sumfield AS sales_to_sum_pos,
    ms.main_discount AS discount_sum_pos,
    ms.card_number,
    dis.qnty AS discount_qty,
    COALESCE(dis.discount_amount, 0::numeric) AS discount_amount,
        CASE
            WHEN ms.main_discount = 0::numeric THEN ms.sumfield
            ELSE dis.discount_amount / ms.main_discount * ms.sumfield
        END AS to_portion,
        CASE
            WHEN dis.discount_identifier IS NULL AND dis.discount_type = 'ROUND'::text AND dis.discount_amount > 0::numeric AND dis.discount_amount < 1::numeric THEN 'promotion-628'::text
            WHEN dis.discount_identifier IS NULL AND (dis.discount_type = ANY (ARRAY['PRICE'::text, 'PERCENT'::text])) AND dis.discount_amount > 0::numeric AND ms.card_number IS NOT NULL THEN 'loyalty'::text
            WHEN ms.qnty < 0 THEN 'return'::text
            WHEN ms.main_discount = 0::numeric THEN 'full_price'::text
            ELSE dis.discount_identifier
        END AS discount_ind,
        CASE
            WHEN dis.discount_identifier IS NULL AND dis.discount_type = 'ROUND'::text AND dis.discount_amount > 0::numeric AND dis.discount_amount < 1::numeric THEN 'Округление копеек в заказе, вниз, до целого.'::text
            WHEN dis.discount_identifier IS NULL AND (dis.discount_type = ANY (ARRAY['PRICE'::text, 'PERCENT'::text])) AND dis.discount_amount > 0::numeric AND ms.card_number IS NOT NULL THEN 'Карта лояльности'::text
            WHEN ms.qnty < 0 THEN 'Возвраты'::text
            WHEN ms.main_discount = 0::numeric THEN 'Полная цена'::text
            ELSE dis.discount_name
        END AS discount_name
   FROM main_sales ms
     LEFT JOIN discount dis ON ms.but_num_business_unit = dis.shop_number AND ms.datecommit = dis.sale_time AND ms.cashnum = dis.cash_number AND ms.opnumfield = dis.purchase_number AND ms.numshift = dis.shift_number AND ms.numberfield = dis.position_order
  GROUP BY ms.datecommit::date, ms.but_num_business_unit, ms.id, ms.numberfield, ms.skucode, ms.qnty, ms.sumfield, ms.main_discount, ms.card_number, dis.qnty, COALESCE(dis.discount_amount, 0::numeric),
        CASE
            WHEN ms.main_discount = 0::numeric THEN ms.sumfield
            ELSE dis.discount_amount / ms.main_discount * ms.sumfield
        END,
        CASE
            WHEN dis.discount_identifier IS NULL AND dis.discount_type = 'ROUND'::text AND dis.discount_amount > 0::numeric AND dis.discount_amount < 1::numeric THEN 'promotion-628'::text
            WHEN dis.discount_identifier IS NULL AND (dis.discount_type = ANY (ARRAY['PRICE'::text, 'PERCENT'::text])) AND dis.discount_amount > 0::numeric AND ms.card_number IS NOT NULL THEN 'loyalty'::text
            WHEN ms.qnty < 0 THEN 'return'::text
            WHEN ms.main_discount = 0::numeric THEN 'full_price'::text
            ELSE dis.discount_identifier
        END,
        CASE
            WHEN dis.discount_identifier IS NULL AND dis.discount_type = 'ROUND'::text AND dis.discount_amount > 0::numeric AND dis.discount_amount < 1::numeric THEN 'Округление копеек в заказе, вниз, до целого.'::text
            WHEN dis.discount_identifier IS NULL AND (dis.discount_type = ANY (ARRAY['PRICE'::text, 'PERCENT'::text])) AND dis.discount_amount > 0::numeric AND ms.card_number IS NOT NULL THEN 'Карта лояльности'::text
            WHEN ms.qnty < 0 THEN 'Возвраты'::text
            WHEN ms.main_discount = 0::numeric THEN 'Полная цена'::text
            ELSE dis.discount_name
        END;
