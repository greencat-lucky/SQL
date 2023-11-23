--Prices and Discounts dashboard
--Permanent

With a as (Select purch_grp,
    day,
    mdl_num_model_r3,
    mod_name_eng,
    fam_num_family,
    fam_name_eng,
    sport,
    case when promo_price is Null then round(sales_price*total_qty,0) else round(promo_price*total_qty,0) end as total_value,
    total_qty,
    round(1-(coalesce(promo_price,sales_price)/sales_price),2) as discount,
    case 
      when promo_price is Null then 'no discount'
      when (round(1-(coalesce(promo_price,sales_price)/sales_price),2)<0.2 
        and round(1-(coalesce(promo_price,sales_price)/sales_price),2)>0) then '1-20%' 
      when round(1-(coalesce(promo_price,sales_price)/sales_price),2)<0.4 then '20-40%'
      when round(1-(coalesce(promo_price,sales_price)/sales_price),2)<0.6 then '40-60%'
      when round(1-(coalesce(promo_price,sales_price)/sales_price),2)<0.8 then '60-80%' 
      when round(1-(coalesce(promo_price,sales_price)/sales_price),2)<1 then '80-99%'
      else '100%' 
    end as bucket
  From (Select price.day,
        sum(stock.qty) as total_qty,
        model.mdl_num_model_r3,
        --stock.sku_num_sku_r3,
        model.mod_name_eng,
        model.fam_num_family,
        model.fam_name_eng,
        sport.unv_name_eng_dmi as sport,
        purch.purch_grp,
        coalesce(round(avg_price_sku.avg_sales_price_sku,1),price.sales_price) as sales_price,
        coalesce(round(avg_price_sku.avg_promo_price_sku,1),price.promo_price) as promo_price
    FROM `data-ru-2dlj.finance_wh.raw_stock_wh_per_sku_new_snapshot` stock
    Left join `finance_wh.d_sku` sku
      on stock.sku_num_sku_r3=sku.sku_num_sku_r3
    Left Join `finance_wh.d_model` model
      on sku.mdl_num_model_r3=model.mdl_num_model_r3
    Left join `data-ru-2dlj.dmi_wh.d_univers_dmi` sport
      on sport.fam_num_family=model.fam_num_family
    Left Join (Select 
        d.day_id_day as day,
        price.material_id as model,
        avg(case when price.price_type='sales_price' then price.price else Null end) as sales_price,
        avg(case when price.price_type='promo_price' then price.price else Null end) as promo_price,
        max(case when price.price_type='promo_price' then price.date_valid_from else Null end) as start_date_promo_price
    From `data-ru-2dlj.dmi_wh.price_history` price
      Inner Join `finance_wh.d_day` d
        on d.day_id_day between price.date_valid_from and price.date_valid_to
        and d.day_id_day=current_date()
      Where price.type_material='01'
      Group by 1,2) as price
      on sku.mdl_num_model_r3=price.model
    Left Join (Select 
        d.day_id_day as day,
        price.material_id as sku,
        avg(case when price.price_type='sales_price' then price.price else Null end) as sales_price_sku,
        avg(case when price.price_type='promo_price' then price.price else Null end) as promo_price_sku,
        max(case when price.price_type='promo_price' then price.date_valid_from else Null end) as start_date_promo_price_sku
    From `data-ru-2dlj.dmi_wh.price_history` price
      Inner Join `finance_wh.d_day` d
        on d.day_id_day between price.date_valid_from and price.date_valid_to
        and d.day_id_day=current_date()
      Where price.type_material='02'
      Group by 1,2) as price_sku
      on stock.sku_num_sku_r3=price_sku.sku
    Left join `data-ru-2dlj.dmi_wh.purch_grp` purch
      on model.fam_num_family=purch.fam_num_family
    Left join (Select 
        d.day_id_day as day,
        sku.mdl_num_model_r3,
        avg(case when price.price_type='sales_price' then price.price else Null end) as avg_sales_price_sku,
        avg(case when price.price_type='promo_price' then price.price else Null end) as avg_promo_price_sku,
    From `data-ru-2dlj.dmi_wh.price_history` price
      Inner Join `finance_wh.d_day` d
        on d.day_id_day between price.date_valid_from and price.date_valid_to
        and d.day_id_day=current_date()
      Left join `data-ru-2dlj.finance_wh.d_sku` sku
        on sku.sku_num_sku_r3=price.material_id
      Left Join `data-ru-2dlj.finance_wh.raw_stock_wh_per_sku_new_snapshot` stock
        on stock.sku_num_sku_r3=sku.sku_num_sku_r3
      Where price.type_material='02'
        and stock.qty>0
        and stock.stock_date=current_date()-1
      Group by 1,2) as avg_price_sku
      on avg_price_sku.mdl_num_model_r3=sku.mdl_num_model_r3
    Where stock.stock_date=current_date()-1
      and stock.qty>0
      and stock.warehouse_id='W049'
      and (price.sales_price is not Null or price_sku.sales_price_sku is not Null)
      --and price.promo_price is Null --or price_sku.promo_price_sku is not Null)
      and sport.unv_name_eng_dmi<>'FURNITURE & FIXURE'
      and model.fam_num_family not in (34963, 11901)
      --and model.mdl_num_model_r3 in (1042303,8328663)
    Group by 1,3,4,5,6,7,8,9,10))

Select 
    a.purch_grp,
    a.day,
    a.mdl_num_model_r3,
    a.mod_name_eng,
    a.fam_num_family,
    a.fam_name_eng,
    a.sport,
    a.total_value,
    a.total_qty,
    a.discount,
    a.bucket,
    case when sales_buckets.sales_bucket is Null then 'No sales last 5+ weeks' else sales_buckets.sales_bucket end as sales_buckets,
    sales_buckets.sales_qty_last_week,
    sales_buckets.sales_qty_n_1_2,
    sales_buckets.sales_qty_n_3_4,
    sales_buckets.sales_qty_n_5
From a
  Left join (Select week_bucket.mdl_num_model_r3,
  Case when week_bucket.sales_qty_last_week>0 then 'Sales last week'
      when (week_bucket.sales_qty_n_1_2=0 and week_bucket.sales_qty_n_3_4>0) then 'No sales last 1-2 weeks'
      when (week_bucket.sales_qty_n_3_4=0 and week_bucket.sales_qty_n_1_2=0 and week_bucket.sales_qty_n_5>0) then 'No sales last 3-4 weeks' 
      when (week_bucket.sales_qty_n_3_4=0 and week_bucket.sales_qty_n_1_2=0 and week_bucket.sales_qty_n_5=0) then 'No sales last 5+ weeks' else 'Sales in previous 4 weeks' end as sales_bucket,
  week_bucket.sales_qty_last_week,
  week_bucket.sales_qty_n_1_2,
  week_bucket.sales_qty_n_3_4,
  week_bucket.sales_qty_n_5
From (Select sales.mdl_num_model_r3,  --current week 7
  sum(case when d.wee_num_week=extract(week from current_date())-1 then sales.qty_goods else 0 end) as sales_qty_last_week, --week 6
  sum(case when (d.wee_num_week=extract(week from current_date())-1 or d.wee_num_week=extract(week from current_date())-2) then
               sales.qty_goods else 0 end) as sales_qty_n_1_2,      --week 6 and 5
  sum(case when (d.wee_num_week=extract(week from current_date())-3 or d.wee_num_week=extract(week from current_date())-4) then
               sales.qty_goods else 0 end) as sales_qty_n_3_4,      --week 4 and 3
  sum(case when d.wee_num_week<extract(week from current_date())-4 then
               sales.qty_goods else 0 end) as sales_qty_n_5,      --week 2 and 1 of 2023            
From `data-ru-2dlj.finance_wh.raw_sales_per_model` sales
  Inner Join `finance_wh.d_day` d
  on sales.date=d.day_id_day
Where sales.date>=current_date()-185
Group by 1) as week_bucket) as sales_buckets
on sales_buckets.mdl_num_model_r3=a.mdl_num_model_r3
