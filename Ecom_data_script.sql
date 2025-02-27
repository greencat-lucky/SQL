

with order_history as (SELECT oh.order_id,
			coalesce(dsn.but_num_business_unit,ds.but_num_business_unit) as store_num,
            coalesce(dsn.shop_name_ru,ds.shop_name_ru) AS wh_name,
            min(date_trunc('second'::text, oh.draft_dt)) AS draft_dt,
            min(date_trunc('second'::text, oh.payment_waiting_dt)) AS payment_waiting_dt,
            min(date_trunc('second'::text, oh.new_dt)) AS new_dt,
            min(date_trunc('second'::text, oh.sborka_dt)) AS in_progress_dt,
            min(date_trunc('second'::text, oh.ready_to_recieve_dt)) AS ready_to_receive_dt,
            min(date_trunc('second'::text, oh.in_delivery_dt)) AS in_delivery_dt,
            min(date_trunc('second'::text, oh.done_dt)) AS done_dt,
            min(date_trunc('second'::text, oh.rejected_dt)) AS rejected_dt,
            min(date_trunc('second'::text, oh.delivery_ready_waiting_dt)) AS delivery_ready_waiting_dt,
            min(date_trunc('second'::text, oh.issue_dt)) AS issue_dt,
            min(date_trunc('second'::text, oh.return_dt)) AS return_dt,
            min(date_trunc('second'::text, oh.erp_error_dt)) AS erp_error_dt,
            min(date_trunc('second'::text, oh.cdek_acceptance_dt)) AS cdek_acceptance_dt,
            min(date_trunc('second'::text, oh.cdek_final_status_dt)) AS cdek_final_status_dt
		From (SELECT esh.order_id,
                    NULL::timestamp without time zone AS draft_dt,
                    NULL::timestamp without time zone AS payment_waiting_dt,
                    min(esh.sys_creation_date) FILTER (WHERE esh.status = 'Новый'::text) AS new_dt,
                    min(esh.sys_creation_date) FILTER (WHERE esh.status = 'Сборка'::text) AS sborka_dt,
                    min(esh.sys_creation_date) FILTER (WHERE esh.status = ANY (ARRAY['Собран'::text, 'ГотовКВыдаче'::text])) AS ready_to_recieve_dt,
                    min(esh.sys_creation_date) FILTER (WHERE esh.status = 'ВДоставке'::text) AS in_delivery_dt,
                    min(esh.sys_creation_date) FILTER (WHERE esh.status = 'Выдан'::text) AS done_dt,
                    min(esh.sys_creation_date) FILTER (WHERE esh.status = 'Отменен'::text) AS rejected_dt,
                    min(esh.sys_creation_date) FILTER (WHERE esh.status = ANY (ARRAY['Проблема'::text, 'СобранЧастично'::text, 'ТребуетПроверки'::text])) AS issue_dt,
                    NULL::timestamp without time zone AS delivery_ready_waiting_dt,
                    min(esh.sys_creation_date) FILTER (WHERE esh.status = 'Возврат'::text) AS return_dt,
                    NULL::timestamp without time zone AS erp_error_dt,
                    NULL::timestamp without time zone AS cdek_acceptance_dt,
                    NULL::timestamp without time zone AS cdek_final_status_dt
                   FROM ods_ecom.erp_status_history esh
                  GROUP BY esh.order_id
                UNION ALL
                 SELECT loh.id,
                    NULL::timestamp without time zone AS draft_dt,
                    min(loh.status_update_date) FILTER (WHERE loh.status::text = 'PAYMENT_WAITING'::text) AS payment_waiting_dt,
                    min(loh.status_update_date) FILTER (WHERE loh.status::text = 'NEW'::text) AS new_dt,
                    min(loh.status_update_date) FILTER (WHERE loh.status::text = 'IN_PROGRESS'::text) AS in_progress_dt,
                    min(loh.status_update_date) FILTER (WHERE loh.status::text = ANY (ARRAY['READY_TO_RECEIVE'::character varying::text, 'DELIVERY_READY'::character varying::text])) AS ready_to_recieve_dt,
                    min(loh.status_update_date) FILTER (WHERE loh.status::text = 'IN_DELIVERY_PROCESS'::text) AS in_delivery_dt,
                    min(loh.status_update_date) FILTER (WHERE loh.status::text = 'DONE'::text) AS done_dt,
                    min(loh.status_update_date) FILTER (WHERE loh.status::text = 'REJECTED'::text) AS rejected_dt,
                    min(loh.status_update_date) FILTER (WHERE loh.status::text = 'ANOMALY'::text) AS issue_dt,
                    min(loh.status_update_date) FILTER (WHERE loh.status::text = ANY (ARRAY['DELIVERY_SEND_ERROR'::character varying::text])) AS delivery_ready_waiting_dt,
                    NULL::timestamp without time zone AS return_dt,
                    min(loh.status_update_date) FILTER (WHERE loh.status::text = 'ERP_TRANSFER_ERROR'::text) AS erp_error_dt,
                    NULL::timestamp without time zone AS cdek_acceptance_dt,
                    NULL::timestamp without time zone AS cdek_final_status_dt
                   FROM ods_ecom.lm_order_h loh
                  GROUP BY loh.id
                UNION ALL
                 SELECT osh.order_id,
                    min(osh.sys_creation_date) FILTER (WHERE osh.status = 'Черновик'::text) AS draft_dt,
                    min(osh.sys_creation_date) FILTER (WHERE osh.status = 'Ожидает оплаты'::text) AS payment_waiting_dt,
                    min(osh.sys_creation_date) FILTER (WHERE osh.status = 'Новый'::text) AS new_dt,
                    min(osh.sys_creation_date) FILTER (WHERE osh.status = 'В сборке'::text) AS in_progress_dt,
                    min(osh.sys_creation_date) FILTER (WHERE osh.status = ANY (ARRAY['Готов к выдаче'::text, 'Готов к отправке'::text])) AS ready_to_recieve_dt,
                    min(osh.sys_creation_date) FILTER (WHERE osh.status = 'В пути'::text) AS in_delivery_dt,
                    min(osh.sys_creation_date) FILTER (WHERE osh.status = 'Выполнен'::text) AS done_dt,
                    min(osh.sys_creation_date) FILTER (WHERE osh.status = ANY (ARRAY['Отменен'::text, 'Ожидает отмены'::text])) AS rejected_dt,
                    min(osh.sys_creation_date) FILTER (WHERE osh.status = 'Аномалия'::text) AS issue_dt,
                    min(osh.sys_creation_date) FILTER (WHERE osh.status = 'Ожидание отправки в ТК'::text) AS delivery_ready_waiting_dt,
                    NULL::timestamp without time zone AS return_dt,
                    NULL::timestamp without time zone AS erp_error_dt,
                    NULL::timestamp without time zone AS cdek_acceptance_dt,
                    NULL::timestamp without time zone AS cdek_final_status_dt
                   FROM ods_ecom.order_status_history osh
                  GROUP BY osh.order_id
                UNION ALL
                 SELECT lm_order.id,
                    lm_order.sys_creation_date AS draft_dt,
                    NULL::timestamp without time zone AS payment_waiting_dt,
                    COALESCE(lm_order.create_erp_date,
                        CASE
                            WHEN lm_order.sys_creation_date <= '2024-07-08 00:00:00'::timestamp without time zone THEN lm_order.sys_creation_date
                            ELSE NULL::timestamp without time zone
                        END) AS new_dt,
                    NULL::timestamp without time zone AS in_progress_dt,
                    NULL::timestamp without time zone AS ready_to_recieve_dt,
                    NULL::timestamp without time zone AS in_delivery_dt,
                    NULL::timestamp without time zone AS done_dt,
                    NULL::timestamp without time zone AS rejected_dt,
                    NULL::timestamp without time zone AS anomaly_dt,
                    NULL::timestamp without time zone AS delivery_ready_waiting_dt,
                    NULL::timestamp without time zone AS return_dt,
                    NULL::timestamp without time zone AS erp_error_dt,
                    NULL::timestamp without time zone AS cdek_acceptance_dt,
                    NULL::timestamp without time zone AS cdek_final_status_dt
                   FROM ods_ecom.lm_order
                UNION ALL
                 SELECT dsh.order_id AS id,
                    NULL::timestamp without time zone AS draft_dt,
                    NULL::timestamp without time zone AS payment_waiting_dt,
                    NULL::timestamp without time zone AS new_dt,
                    NULL::timestamp without time zone AS sborka_dt,
                    NULL::timestamp without time zone AS ready_to_recieve_dt,
                    NULL::timestamp without time zone AS in_delivery_dt,
                    NULL::timestamp without time zone AS done_dt,
                    NULL::timestamp without time zone AS rejected_dt,
                    NULL::timestamp without time zone AS issue_dt,
                    NULL::timestamp without time zone AS delivery_ready_waiting_dt,
                    NULL::timestamp without time zone AS return_dt,
                    NULL::timestamp without time zone AS erp_error_dt,
                    min(dsh.date_time) FILTER (WHERE dsh.status = 'RECEIVED_AT_SHIPMENT_WAREHOUSE'::text) AS cdek_acceptance_dt,
                    min(dsh.date_time) FILTER (WHERE dsh.status = ANY (ARRAY['DELIVERED'::text, 'NOT DELIVERED'::text])) AS cdek_final_status_dt
                   FROM ods_ecom.delivery_status_history dsh
                  GROUP BY dsh.order_id) oh
                 JOIN ods_ecom.lm_order lo ON oh.order_id = lo.id::text
                 left join cds.d_store_new ds on ds.warehouse_id=lo.warehouse_id
                 LEFT JOIN cds.d_store_new dsn ON dsn.lm_ecom_id = lo.pickup_point_id::text
                 group by 1,2,3),
order_history_details as (select ss.time_open_msk,
	ss.time_close_msk,
	ss1.time_open_msk as time_open_msk_1_day,
case when EXTRACT(HOUR FROM oh.new_dt)>=extract(hour from ss.time_open_msk) and EXTRACT(HOUR FROM oh.new_dt)<extract(hour from ss.time_close_msk) then oh.new_dt 
	when oh.new_dt is not null and ss1.time_open_msk is null and EXTRACT(HOUR FROM oh.new_dt)>=extract(hour from ss.time_close_msk) then oh.new_dt::date +INTERVAL '2 day' + ss.time_open_msk
	when EXTRACT(HOUR FROM oh.new_dt)>=extract(hour from ss.time_close_msk) then oh.new_dt::date +INTERVAL '1 day' + ss1.time_open_msk
		when EXTRACT(HOUR FROM oh.new_dt)<extract(hour from ss.time_open_msk) then oh.new_dt::date + ss.time_open_msk
			else '9999-12-30 00:00:00.000' end as time_start_sla,
oh.*
	from order_history oh
left join dev.f_stores_schedule_slt ss
	on oh.new_dt::date>=ss.start_date
	and oh.new_dt::date<=ss.end_date
	and oh.store_num=ss.store_num
left join dev.f_stores_schedule_slt ss1
	on oh.new_dt::date +INTERVAL '1 day'>=ss1.start_date
	and oh.new_dt::date +INTERVAL '1 day'<=ss1.end_date
	and oh.store_num=ss1.store_num)
 SELECT final_table.id,
    final_table.operator_full_name,
    final_table.order_maker_phone,
    final_table.order_maker_email,
    final_table.delivery_city,
    final_table.wh_city,
    final_table.store_num,
    final_table.warehouse,
    final_table.payment_amount,
    final_table.delivery_type,
    final_table.delivery_address,
    final_table.payment_type,
    final_table.payment_status_code,
    final_table.payment_date,
    final_table.cdek_uuid,
    final_table.coment,
    final_table.current_status,
    final_table.creation_hour_period,
    final_table.sys_creation_date,
    final_table.sys_update_date,
    final_table.status_update_date,
    final_table.time_start_sla,
    final_table.draft_dt,
    final_table.payment_waiting_dt,
    final_table.new_dt,
    final_table.in_progress_dt,
    final_table.ready_to_receive_dt,
    final_table.in_delivery_dt,
    final_table.done_dt,
    final_table.rejected_dt,
    final_table.delivery_ready_waiting_dt,
    final_table.issue_dt,
    final_table.return_dt,
    final_table.erp_error_dt,
    final_table.payment_waiting_flg,
    final_table.new_flg,
    final_table.in_progress_flg,
    final_table.ready_to_receive_flg,
    final_table.in_delivery_progress_flg,
    final_table.done_flg,
    final_table.rejected_payment_waiting_flg,
    final_table.rejected_new_flg,
    final_table.rejected_in_progress_flg,
    final_table.rejected_ready_to_recieve_flg,
    final_table.rejected_in_delivery_progress_flg,
    final_table.issue_flg,
    final_table.return_flg,
    final_table.skucode,
    final_table.modelcode,
    final_table.subdepartment_name,
    final_table.department_name,
    final_table.sector_name,
    final_table.modelname,
    final_table.skufullname,
    final_table.skuname,
    final_table.gender,
    final_table.gendercategory,
    final_table.dec_producttype,
    final_table.dec_practicelevel,
    final_table.qty,
    final_table.to_tax_in,
    final_table.to_tax_ex,
    final_table.supplier_cost,
    final_table.pre_ordered_count,
    final_table.pre_to_tax_in,
    final_table.nds,
    final_table.order_source,
    final_table.reject_reason,
        CASE
            WHEN COALESCE(final_table.ready_to_receive_dt, final_table.rejected_dt) IS NOT NULL AND COALESCE(final_table.ready_to_receive_dt, final_table.rejected_dt) < final_table.time_start_sla THEN 0::double precision
            WHEN final_table.time_start_sla IS NOT NULL THEN date_part('epoch'::text, COALESCE(final_table.ready_to_receive_dt, final_table.rejected_dt) - final_table.time_start_sla) / 3600::double precision / 24::double precision
            ELSE NULL::double precision
        END AS sla_complex,
    date_part('epoch'::text, now() - final_table.status_update_date::timestamp with time zone) / 3600::double precision / 24::double precision AS last_status_duration,
    final_table.rsm,
        CASE
            WHEN final_table.reject_reason_name = 'Клиент не пришел, продлевать не стал'::text THEN 'Клиент не пришёл'::text
            WHEN final_table.reject_reason_name IS NULL AND final_table.current_status::text = 'REJECTED'::text THEN 'Причина не указана'::text
            WHEN final_table.reject_reason_name = 'Клиент самостоятельно инициировал отмену заказа'::text THEN 'Клиент сделал запрос об отмене'::text
            ELSE btrim(final_table.reject_reason_name)
        END AS reject_reason_name,
    final_table.tracking_number,
    final_table.payment_deposit_amount,
    final_table.cdek_acceptance_dt,
    final_table.cdek_final_status_dt,
    final_table.delivery_price / 100 AS delivery_price,
    final_table.loyalty_card_number,
    final_table.weight,
    final_table.weight * final_table.qty AS total_sku_weight,
    final_table.applied_promo_code
   FROM (SELECT lo.id,
            lo.operator_full_name,
            lo.order_maker_phone,
            lo.order_maker_email,
            lo.delivery_city_name AS delivery_city,
            dsn.but_num_business_unit AS storeid,
            dsn.shop_name_ru AS storename,
            COALESCE(oh.wh_name, 'Не указан'::text) AS warehouse,
            oh.store_num,
            lo.payment_amount / 100 AS payment_amount,
            lo.delivery_type,
            lo.delivery_address,
            lo.payment_type,
            lo.reject_reason_name,
                CASE
                    WHEN lo.delivery_type::text = 'SHOP'::text AND lo.status::text = 'DONE'::text THEN 'PAID'::text
                    ELSE lo.payment_status_code
                END AS payment_status_code,
                CASE
                    WHEN lo.delivery_type::text = 'SHOP'::text AND lo.status::text = 'DONE'::text THEN lo.status_update_date
                    ELSE COALESCE(lo.payment_date, lo.payment_deposit_date)
                END AS payment_date,
            lo.cdek_uuid,
            replace(lo.employee_comment, '
'::text, ' '::text) AS coment,
            lo.status AS current_status,
            date_part('hour'::text, lo.sys_creation_date) AS creation_hour_period,
            lo.sys_creation_date,
            lo.sys_update_date,
            lo.status_update_date,
            oh.time_start_sla,
            oh.draft_dt,
            oh.payment_waiting_dt,
            oh.new_dt,
            oh.in_progress_dt,
            oh.ready_to_receive_dt,
            oh.in_delivery_dt,
            oh.done_dt,
            oh.rejected_dt,
            oh.issue_dt,
            oh.return_dt,
            oh.erp_error_dt,
            oh.delivery_ready_waiting_dt,
            oh.cdek_acceptance_dt,
            oh.cdek_final_status_dt,
                CASE
                    WHEN COALESCE(oh.payment_waiting_dt, oh.new_dt, oh.in_progress_dt, oh.ready_to_receive_dt, oh.in_delivery_dt, oh.done_dt) IS NOT NULL THEN lo.id
                    ELSE NULL::character varying
                END AS payment_waiting_flg,
                CASE
                    WHEN COALESCE(oh.new_dt, oh.in_progress_dt, oh.ready_to_receive_dt, oh.in_delivery_dt, oh.done_dt) IS NOT NULL THEN lo.id
                    ELSE NULL::character varying
                END AS new_flg,
                CASE
                    WHEN COALESCE(oh.in_progress_dt, oh.ready_to_receive_dt, oh.in_delivery_dt, oh.done_dt) IS NOT NULL THEN lo.id
                    ELSE NULL::character varying
                END AS in_progress_flg,
                CASE
                    WHEN COALESCE(oh.ready_to_receive_dt, oh.in_delivery_dt, oh.done_dt) IS NOT NULL THEN lo.id
                    ELSE NULL::character varying
                END AS ready_to_receive_flg,
                CASE
                    WHEN COALESCE(oh.in_delivery_dt, oh.done_dt) IS NOT NULL AND lo.delivery_type::text <> 'SHOP'::text THEN lo.id
                    ELSE NULL::character varying
                END AS in_delivery_progress_flg,
                CASE
                    WHEN oh.done_dt IS NOT NULL OR lo.status::text = 'DONE'::text THEN lo.id
                    ELSE NULL::character varying
                END AS done_flg,
                CASE
                    WHEN lo.status::text = 'REJECTED'::text AND oh.payment_waiting_dt IS NOT NULL AND COALESCE(oh.new_dt, oh.in_progress_dt, oh.ready_to_receive_dt, oh.in_delivery_dt) IS NULL THEN lo.id
                    ELSE NULL::character varying
                END AS rejected_payment_waiting_flg,
                CASE
                    WHEN lo.status::text = 'REJECTED'::text AND oh.new_dt IS NOT NULL AND COALESCE(oh.in_progress_dt, oh.ready_to_receive_dt, oh.in_delivery_dt) IS NULL THEN lo.id
                    ELSE NULL::character varying
                END AS rejected_new_flg,
                CASE
                    WHEN lo.status::text = 'REJECTED'::text AND oh.in_progress_dt IS NOT NULL AND COALESCE(oh.ready_to_receive_dt, oh.in_delivery_dt) IS NULL THEN lo.id
                    ELSE NULL::character varying
                END AS rejected_in_progress_flg,
                CASE
                    WHEN lo.status::text = 'REJECTED'::text AND COALESCE(oh.ready_to_receive_dt) IS NOT NULL AND oh.in_delivery_dt IS NULL THEN lo.id
                    ELSE NULL::character varying
                END AS rejected_ready_to_recieve_flg,
                CASE
                    WHEN lo.status::text = 'REJECTED'::text AND oh.in_delivery_dt IS NOT NULL THEN lo.id
                    ELSE NULL::character varying
                END AS rejected_in_delivery_progress_flg,
                CASE
                    WHEN COALESCE(oh.issue_dt, oh.erp_error_dt) IS NOT NULL THEN lo.id
                    ELSE NULL::character varying
                END AS issue_flg,
                CASE
                    WHEN oh.return_dt IS NOT NULL THEN lo.id
                    ELSE NULL::character varying
                END AS return_flg,
            dp.skucode,
            dp.modelcode,
            dp.subdepartment_name,
            dp.department_name,
            dp.sector_name,
            dp.modelname,
            dp.skufullname,
            dp.skuname,
            dp.gender,
            dp.gendercategory,
            dp.dec_producttype,
            dp.dec_practicelevel,
            oi.count AS qty,
            oi.human_cost AS to_tax_in,
            oi.human_cost / (1::double precision + COALESCE(oi.nds, dp.vat::bigint)::double precision / 100::double precision) AS to_tax_ex,
            COALESCE(oi.pre_ordered_count, oi.count) AS pre_ordered_count,
            mm.purchase_price_with_transport * oi.count::double precision AS supplier_cost,
            COALESCE(oi.pre_ordered_count::double precision * oi.human_price, oi.human_cost) AS pre_to_tax_in,
            oi.nds,
                CASE
                    WHEN lo.order_source = 'WEB_APP'::text THEN 'WEB SITE'::text
                    ELSE lo.order_source
                END AS order_source,
            lo.reject_reason,
            COALESCE(dsn.rsm, sw.rsm) AS rsm,
            lo.tracking_number,
            COALESCE(sw.city_rus, dsn.city_rus) AS wh_city,
            lo.payment_deposit_amount / 100 AS payment_deposit_amount,
            lo.delivery_price,
            coalesce(mcd.cardnumber,lo.loyalty_card_number) as loyalty_card_number,
            oi.weight,
            lo.applied_promo_code
           FROM ods_ecom.lm_order lo
             LEFT JOIN order_history_details oh ON lo.id::text = oh.order_id
             left join (select mobilephone, cardnumber
             			from restricted.mindbox_clients_data mcd
             			group by 1,2) mcd
             	on mcd.mobilephone=lo.order_maker_phone
             JOIN ods_ecom.order_item oi ON oi.order_id::text = lo.id::text
             LEFT JOIN cds.d_store_new sw ON lo.warehouse_id = sw.warehouse_id
             LEFT JOIN cds.d_product dp ON oi.product_id::text = dp.skucode
             LEFT JOIN cds.d_store_new dsn ON dsn.lm_ecom_id = lo.pickup_point_id::text
             LEFT JOIN cds.f_supplier_purchase_price mm 
             	ON dp.skucode = mm.skucode
            	AND lo.sys_creation_date::date >= mm.date_start 
            	AND lo.sys_creation_date::date <= mm.date_end) final_table