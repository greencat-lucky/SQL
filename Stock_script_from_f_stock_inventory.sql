drop table if exists temp_stock_ru1204;
create temp table temp_stock_ru1204 
(day_id_day date,
 business_unit int,
 sku_num_sku_r3 int,
 stock_type int,
 lifestage int2,
 code char,
 f_quantity int8
)
diststyle key
distkey (sku_num_sku_r3)
;

insert into temp_stock_ru1204
select 
        fsi.inventory_date,	
		fsi.third_number_storage,
		fsi.item_code_r3,
		fsi.stock_type,
        COALESCE(('0'+dsdmh.lifestage)::int,0) as lifestage,
	    dgdwh.sdw_code_abc as ABC_code,		
        max(fsi.picture_quantity) as stock_qty
from cds.f_stock_inventory fsi
Inner Join cds.d_business_unit AS bu           --Joining in order to filter by Country
       ON fsi.third_number_storage = bu.but_num_business_unit 
       and fsi.third_sub_number_storage = bu.but_sub_num_but 
       and fsi.third_type_storage=7            --7-store type (9-warehouse)
       and bu.cnt_idr_country = 167  
left join cds_supply.d_sales_data_material_h dsdmh 
on cast('0'||dsdmh.material_id as int8) = fsi.item_code_r3  --joining lifestages
	and dsdmh.sales_org ='Z018'
    and dsdmh.distrib_channel  = '02'
    and dsdmh.date_begin<=fsi.inventory_date
    and dsdmh.date_end>=fsi.inventory_date
left join cds_supply.d_general_data_warehouse_h dgdwh    --joining abc codes
on cast('0'||dgdwh.sdw_material_id as int8) = fsi.item_code_r3  
	and dgdwh.sdw_sap_source ='PRT'
	and dgdwh.sdw_plant_id = 'W049' -- all codes are the same for the country, based on CAC data
    and dgdwh.date_begin<=fsi.inventory_date
    and dgdwh.date_end>=fsi.inventory_date
where fsi.stock_type=1 
---and fsi.third_number_storage=399          ---just for checking data
and fsi.inventory_date='2022-09-12'          --to replace with current_date-?
and fsi.picture_quantity>0
group by 1,2,3,4,5,6;


drop table if exists temp_prices_h_ru1204;
create temp table temp_prices_h_ru1204
(
material_id int8, 
date_valid_to date, 
date_valid_from date, 
condition_type varchar(20), 
condition_num varchar(20), 
cond_value numeric(11,2)
)
diststyle key
distkey (material_id)
;

insert into temp_prices_h_ru1204 
select 
	cast('0'||ltrim(material_id,'0') as int8),
	date_valid_to,
	date_valid_from,
	condition_type,
	condition_num,
	cond_value 
from 
(select 
		row_number() over(partition by rec.condition_num, rec.date_valid_from order by rec.date_valid_to) as rn, 
		rec.material_id, 
		rec.date_valid_to, 
		rec.date_valid_from,
		rec.condition_type,
		rec.condition_num,
		ci.cond_value
	from cds_supply.d_sales_price_info_record rec
	left join cds_supply.conditions_item ci
	on rec.condition_num = ci.condition_num 
		and ci.sap_source = 'PRT'
		and ci.seq_condition_num = '01'
	where rec.sales_org = 'Z018'
		and rec.condition_type = 'ZP06'
		and rec.sap_source ='PRT'
union all 
select 
		row_number() over(partition by rec.condition_num, rec.date_valid_from order by rec.date_valid_to asc) as rn, 
		rec.material_id, 
		rec.date_valid_to, 
		rec.date_valid_from,
		rec.condition_type,
		rec.condition_num,
		ci.cond_value
	from cds_supply.d_sales_price_info_record rec
	left join cds_supply.conditions_item ci
	on rec.condition_num = ci.condition_num 
		and ci.sap_source = 'PRT'
		and ci.seq_condition_num = '01'
	where rec.sales_org = 'Z018'
		and rec.condition_type = 'ZP05'
		and rec.sap_source ='PRT'
)
where rn =1
;

drop table if exists final_stock_ru1204;
create temp table final_stock_ru1204
(day_id_day date,
 business_unit int,
 sku_num_sku_r3 int,
 stock_type int,
 lifestage int2,
 code char,
 f_quantity int8,
 dmi_price numeric(20,2)
 )
diststyle key
distkey (sku_num_sku_r3)
;

insert into final_stock_ru1204
select 
 day_id_day,
 business_unit,
 sku_num_sku_r3,
 stock_type,
 lifestage,
 code,
 f_quantity,
 dmi_price
 from
(
select 
	main.day_id_day,
 	main.business_unit,
 	main.sku_num_sku_r3,
 	main.stock_type,
 	main.lifestage,
 	main.code,
 	main.f_quantity,
	COALESCE(dspir2.cond_value, dspir1.cond_value, dspir4.cond_value, dspir3.cond_value, 0.0) dmi_price,                 --changed from cases
	row_number() over (partition by main.day_id_day, main.business_unit, main.sku_num_sku_r3 order by dspir2.date_valid_from desc) as rn
from temp_stock_ru1204 main
left join 
	(select 
		sku_num_sku_r3, mdl_num_model_r3,
		 row_number() over (partition by sku_num_sku_r3 order by sku_date_end desc) as rn
		 from cds.d_sku) sku
on main.sku_num_sku_r3 = sku.sku_num_sku_r3 and sku.rn = 1
left join 
	temp_prices_h_ru1204 dspir1
	on dspir1.material_id = main.sku_num_sku_r3 
	and dspir1.condition_type = 'ZP06'
	and dspir1.date_valid_to >=main.day_id_day
	and dspir1.date_valid_from <=main.day_id_day
left join 
	temp_prices_h_ru1204 dspir2
	on dspir2.material_id = main.sku_num_sku_r3 
	and dspir2.condition_type = 'ZP05'
	and dspir2.date_valid_to >=main.day_id_day
	and dspir2.date_valid_from <=main.day_id_day
left join 
	temp_prices_h_ru1204 dspir3
	on dspir3.material_id = sku.mdl_num_model_r3 
	and dspir3.condition_type = 'ZP06'
	and dspir3.date_valid_to >=main.day_id_day
	and dspir3.date_valid_from <=main.day_id_day
left join 
	temp_prices_h_ru1204 dspir4
	on dspir4.material_id = sku.mdl_num_model_r3 
	and dspir4.condition_type = 'ZP05'
	and dspir4.date_valid_to >=main.day_id_day
	and dspir4.date_valid_from <=main.day_id_day
)
where rn=1;
