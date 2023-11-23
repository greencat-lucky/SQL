--Forecasted stock, based on week start stock and zem01 forecasts

Select   
        sku_num_sku_r3,
        model_name,
        fam_num_family,
        fam_name_eng,
        sport,
        purch_grp,   
        qty,
        forecast_week,
        forecast_qty,
        qty-fcs_running as end_of_week_stock,
        date
From(
Select stock.sku_num_sku_r3,
        stock.qty,
        fcs.date,
        fcs.forecast_week,
        fcs.forecast_qty,
        concat(model.mod_name_eng,' ',coalesce(alt_sku.grid_size,'')) as model_name,
        model.fam_num_family,
        model.fam_name_eng,
        sport.unv_name_eng_dmi as sport,
        purch.purch_grp,
        sum(fcs.forecast_qty) over (partition by stock.sku_num_sku_r3 order by fcs.forecast_week asc) fcs_running
From `finance_wh.raw_stock_wh_per_sku_new_snapshot` stock
Left join `finance_wh.d_sku` sku
        on stock.sku_num_sku_r3=sku.sku_num_sku_r3
Left Join `finance_wh.d_model` model
        on sku.mdl_num_model_r3=model.mdl_num_model_r3
Left join `data-ru-2dlj.dmi_wh.d_univers_dmi` sport
        on sport.fam_num_family=model.fam_num_family
Left Join `finance_wh.d_sku_alt` alt_sku
        on alt_sku.sku_num_sku_r3=stock.sku_num_sku_r3
Left join `data-ru-2dlj.dmi_wh.f_apo_forecast` fcs
     on fcs.sku_num_sku_r3=stock.sku_num_sku_r3
Left join `data-ru-2dlj.dmi_wh.purch_grp` purch
  on model.fam_num_family=purch.fam_num_family
Inner join (Select d.day_id_day as day
                        From `finance_wh.d_day` d
                        Where d.wee_id_week=cast(concat(extract(year from current_date()),extract(week from current_date())) as integer)-1
                        and d.day_num_weekday=6) saturday
on saturday.day=stock.stock_date
Where stock.stock_date>=current_date()-10
        and stock.business_unit=49 and stock.qty>0
        and fcs.forecast_week<202323
        and sport.unv_name_eng_dmi<>'FURNITURE & FIXURE'
        and model.fam_num_family not in (34963, 11901))
--and stock.sku_num_sku_r3 in (2889231)
Where forecast_qty+qty-fcs_running>0