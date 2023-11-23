--Olga Shatokhina
--Prices and Discounts dashboard
--Permanent

Select stock.stock_date,
    --stock.lifestage,
    --stock.code,
    sum(stock.qty) as total_qty,
    model.mdl_num_model_r3,
    --stock.sku_num_sku_r3,
    model.mod_name_eng,
    model.fam_num_family,
    model.fam_name_eng,
    sport.unv_name_eng_dmi as sport,
    purch.purch_grp,
    price.day,
    coalesce(price.sales_price,round(avg_price_sku.avg_sales_price_sku,1)) as sales_price,
    coalesce(price.promo_price,round(avg_price_sku.avg_promo_price_sku,1)) as promo_price,
    case when price.start_date_promo_price=current_date() then 'Active from today' else
      case when price.start_date_promo_price>=current_date()-3 then 'Less than 3 days ago' else
      case when (price.start_date_promo_price<=current_date()-7 and price.start_date_promo_price>current_date()-30) then 'More than 7 days ago' else
      case when price.start_date_promo_price<=current_date()-30 then 'More than 30 days ago' else '' end end end end as recent_key,
    coalesce(price.start_date_promo_price,price_sku.start_date_promo_price_sku) as start_date_promo_price,
    disc.alert
    --price_sku.sales_price_sku,
    --price_sku.promo_price_sku,
    --price_sku.start_date_promo_price_sku
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
Left join `data-ru-2dlj.dmi_wh.f_price_discrepancies` disc
  on disc.model=model.mdl_num_model_r3
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
  and (price.promo_price is not Null or price_sku.promo_price_sku is not Null)
  and sport.unv_name_eng_dmi<>'FURNITURE & FIXURE'
  and model.fam_num_family not in (34963, 11901)
  --and model.mdl_num_model_r3 in (1042303,8328663)
Group by 1,3,4,5,6,7,8,9,10,11,12,13,14