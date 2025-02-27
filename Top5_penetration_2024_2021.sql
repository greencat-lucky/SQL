------
with a as (select
	date_part('week',fso.date_2024) as week_2024,
	fso.sector,
	fso.dec_producttype as typology,
	checks_sector.checks_sector,
	count(distinct check_id) as checks_typology
from dev.f_sales_2024_os fso
left join (select 
			fso.sector,
			count(distinct check_id) as checks_sector
			from dev.f_sales_2024_os fso
			where  fso.store_num not in (1,2,3)
				and date_part('week',fso.date_2024)=date_part('week', now())-1
			group by 1) as checks_sector
	on checks_sector.sector=fso.sector
where  fso.store_num not in (1,2,3)
	and date_part('week',fso.date_2024)=date_part('week', now())-1
group by 1,2,3,4),
b as (
select week_2024,
	sector,
	typology,
	checks_typology,
	row_number() over (partition by sector order by checks_typology desc) as rank_typology,
	checks_sector,
	dense_rank() over(order by checks_sector desc) as rank_sector
from a),
 c as (SELECT 
	date_part('week',ftda.the_date_transaction::date) AS week_2021,
	d.sector_name_new AS sector,
	ftda.the_transaction_id as check_num,
    ftda.but_num_business_unit AS store_num,
    dto.desport_producttype_new_level2 as typology,
    round(sum(ftda.f_to_tax_in), 0) AS to_sum,
    sum(ftda.f_qty_item) AS qty_sum
   FROM history.f_transaction_detail_all ftda
     LEFT JOIN history.d_day_new ddn ON ddn.day_id_day_comp = ftda.the_date_transaction::date AND date_part('year'::text, ddn.day_id_day) = 2024::double precision
     LEFT JOIN cds.d_store_new s ON s.but_num_business_unit = ftda.but_num_business_unit
     LEFT JOIN dev.d_old_new_family_match d ON d.old_fam_num = ftda.fam_num_family
     Left join dev.d_typology_2021_os dto 
     	on dto.model_num=ftda.mdl_num_model_r3
     JOIN (SELECT d.store_num
           FROM dev.f_sales_2024_os d
          Where date_part('week',d.date_2024)=date_part('week',now()::date)-1
          GROUP BY 1) open_stores ON open_stores.store_num = ftda.but_num_business_unit
  WHERE 1=1 
  and date_part('week',ftda.the_date_transaction::date)=date_part('week',now()::date)-1 and date_part('year',ftda.the_date_transaction::date)=2021
  and ftda.the_to_type = 'offline'::text AND ftda.tdt_item_type = 'Stock'::text 
  and ftda.the_transaction_status = 'finished'::text 
  AND d.sector_name_new::text not in ('391- OTHER SERVICES WORKSHOP', '90- SERVICES', '89 - FURNITURE & FIXTURE','14 - Diversification')
  AND d.new_family_name::text <> '10045 - GIFT CARDS'::text
  GROUP BY 1,2,3,4,5),
  d as (select
  		week_2021,
  		c.sector,
  		typology,
  		count(distinct(check_num)) as checks_typology,
  		sector_checks.checks_sector
  	from c
  left join (select sector, 
  					count(distinct(check_num)) as checks_sector
  					from c 
  			group by 1) sector_checks
	  on sector_checks.sector=c.sector
  where 1=1
  group by 1,2,3,5),
  e as (select week_2021,
	sector,
	typology,
	checks_typology,
	row_number() over (partition by sector order by checks_typology desc) as rank_typology,
	checks_sector,
	dense_rank() over(order by checks_sector desc) as rank_sector
  from d)
  select '2021'::int as year_num, e.*
  from e
  where rank_typology<=5 and rank_sector<=6
union all
select '2024'::int as year_num, b.*
from b
where rank_typology<=5 and rank_sector<=6;
----------

 select '2024'::int as year_num, b.*
 from 
	(select week_2024,
		sector,
		typology,
		checks_typology,
		row_number() over (partition by sector order by checks_typology desc) as rank_typology,
		checks_sector,
		dense_rank() over(order by checks_sector desc) as rank_sector
	from 	
			 (select distinct 
				date_part('week',fso.date_2024) as week_2024,
				fso.sector,
				fso.dec_producttype as typology,
				count(distinct check_id)  over (partition by fso.sector) as checks_sector,
			   	count(distinct check_id)  over (partition by fso.sector, fso.dec_producttype) as checks_typology
			from dev.f_sales_2024_os fso
			where  fso.store_num not in (1,2,3)
				and date_part('week',fso.date_2024)=date_part('week', now())-1) a
)b
where rank_typology<=5 and rank_sector<=6
union all
select '2021'::int as year_num, b.*
 from 
	(select week_2021,
			sector,
			typology,
			checks_typology,
			row_number() over (partition by sector order by checks_typology desc) as rank_typology,
			checks_sector,
			dense_rank() over(order by checks_sector desc) as rank_sector
	from (SELECT distinct
		date_part('week',ftda.the_date_transaction::date) AS week_2021,
		d.sector_name_new AS sector,
		dto.desport_producttype_new_level2 as typology,
	    count(distinct ftda.the_transaction_id)  over (partition by d.sector_name_new) as checks_sector,
		count(distinct ftda.the_transaction_id)  over (partition by d.sector_name_new, dto.desport_producttype_new_level2) as checks_typology
	   FROM history.f_transaction_detail_all ftda
	     LEFT JOIN dev.d_old_new_family_match d ON d.old_fam_num = ftda.fam_num_family
	     Left join dev.d_typology_2021_os dto 
	     	on dto.model_num=ftda.mdl_num_model_r3
	     JOIN (SELECT d.store_num
	           FROM dev.f_sales_2024_os d
	          Where date_part('week',d.date_2024)=date_part('week',now()::date)-1
	          GROUP BY 1) open_stores ON open_stores.store_num = ftda.but_num_business_unit
	  WHERE 1=1 
	  and date_part('week',ftda.the_date_transaction::date)=date_part('week',now()::date)-1 and date_part('year',ftda.the_date_transaction::date)=2021
	  and ftda.the_to_type = 'offline'::text AND ftda.tdt_item_type = 'Stock'::text 
	  and ftda.the_transaction_status = 'finished'::text 
	  AND d.sector_name_new::text not in ('391- OTHER SERVICES WORKSHOP', '90- SERVICES', '89 - FURNITURE & FIXTURE','14 - Diversification')
	  AND d.new_family_name::text <> '10045 - GIFT CARDS'::text ) a
)b
where rank_typology<=5 and rank_sector<=6;
  





















