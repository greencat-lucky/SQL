----top5 by topology LW, LW LY, checks, qty, to

with year_2025 as (SELECT '2025' as year,
	date_part('week'::text, fso.date_2024) AS week_num,
    fso.macro_level_scm,
    fso.sector,
    fso.dec_producttype as typology,
    sum(fso.to_2024) AS to_sum,
    sum(fso.qty_2024) AS qty_sum_all,
    sum(CASE
            WHEN fso.qty_2024 > 0 then fso.qty_2024 else 0 end) as qty_sum_only_sales,
    count(distinct fso.check_id) AS num_checks_typology_all,
    count(DISTINCT
        CASE
            WHEN fso.qty_2024 > 0::double precision THEN fso.check_id
            ELSE NULL::bigint
        END) AS num_checks_typology_only_sales,
    macro.num_checks_macro_sector_all,   
    macro.num_checks_macro_sector_only_sales
 FROM dev.f_sales_2024_os fso
		 Inner join (select distinct store_num,
				      date_part('week',fso2.date_2024) as week_num
				      from dev.f_sales_2024_os fso2
				      where date_part('year',fso2.date_2024)=2024 and date_part('week',fso2.date_2024) = date_part('week', now())-1
				           and store_num not in (1,2,3) and fso2.date_2024<'2024-12-30'
				           ) week_store
				 on week_store.week_num=date_part('week'::text, fso.date_2024)
				 and week_store.store_num=fso.store_num
		left join (SELECT date_part('week'::text, fso_1.date_2024) AS week_num,
				            fso_1.macro_level_scm AS macro_sector,
				            count(distinct fso_1.check_id) as num_checks_macro_sector_all,
				         	count(DISTINCT
				                CASE
				                    WHEN fso_1.qty_2024 > 0::double precision THEN fso_1.check_id
				                    ELSE NULL::bigint
				                END) AS num_checks_macro_sector_only_sales
				          FROM dev.f_sales_2024_os fso_1
				      inner join (select distinct store_num,
						      date_part('week',fso2.date_2024) as week_num
						      from dev.f_sales_2024_os fso2
						      where date_part('year',fso2.date_2024)=2024 and date_part('week',fso2.date_2024) = date_part('week', now())-1 
						           and store_num not in (1,2,3) and fso2.date_2024<'2024-12-30') week_store
						 on week_store.week_num=date_part('week'::text, fso_1.date_2024)
						 and week_store.store_num=fso_1.store_num
					where date_part('year', fso_1.date_2024)=2025 and fso_1.macro_level_scm in ('WINTER SPORTS','OUTDOOR','WATER SPORT','FITNESS','RUN& KID& WALKING')
					group by 1,2) macro
			on macro.week_num=date_part('week'::text, fso.date_2024)
			and macro.macro_sector=fso.macro_level_scm 
where date_part('year', fso.date_2024)=2025 and fso.macro_level_scm in ('WINTER SPORTS','OUTDOOR','WATER SPORT','FITNESS','RUN& KID& WALKING')
group by 1,2,3,4,5,11,12),
year_2025_r as(
			select rank() over (partition by macro_level_scm, sector order by  to_sum desc) as rn,
			year_2025.*
			from year_2025),
year_2024 as (SELECT '2024' as year,
	date_part('week'::text, fso.date_2024) AS week_num,
    fso.macro_level_scm,
    fso.sector,
    fso.dec_producttype as typology,
    sum(fso.to_2024) AS to_sum,
    sum(fso.qty_2024) as qty_sum_all,
    sum(CASE
            WHEN fso.qty_2024 > 0 then fso.qty_2024 else 0 end) AS qty_sum_only_sales,
    count(distinct fso.check_id) as num_typology_sector_all,
    count(DISTINCT
        CASE
            WHEN fso.qty_2024 > 0::double precision THEN fso.check_id
            ELSE NULL::bigint
        END) AS num_checks_typology_only_sales,
    macro.num_checks_macro_sector_all,  
    macro.num_checks_macro_sector_only_sales
 FROM dev.f_sales_2024_os fso
left join (SELECT date_part('week'::text, fso_1.date_2024) AS week_num,
		            fso_1.macro_level_scm AS macro_sector,
		            count(distinct fso_1.check_id) as num_checks_macro_sector_all,
		         	count(DISTINCT
		                CASE
		                    WHEN fso_1.qty_2024 > 0::double precision THEN fso_1.check_id
		                    ELSE NULL::bigint
		                END) AS num_checks_macro_sector_only_sales
		          FROM dev.f_sales_2024_os fso_1
		      inner join (select distinct store_num,
				      date_part('week',fso2.date_2024) as week_num
				      from dev.f_sales_2024_os fso2
				      where date_part('year',fso2.date_2024)=2024 and date_part('week',fso2.date_2024) = date_part('week', now())-1
				          	 and store_num not in (1,2,3) and fso2.date_2024<'2024-12-30') week_store
				 on week_store.week_num=date_part('week'::text, fso_1.date_2024)
				 and week_store.store_num=fso_1.store_num
			where date_part('year', fso_1.date_2024)=2024 and fso_1.macro_level_scm in ('WINTER SPORTS','OUTDOOR','WATER SPORT','FITNESS','RUN& KID& WALKING')
			group by 1,2) macro
	on macro.week_num=date_part('week'::text, fso.date_2024)
	and macro.macro_sector=fso.macro_level_scm 
where date_part('year', fso.date_2024)=2024 and fso.date_2024<'2024-12-30'
	 and date_part('week',fso.date_2024) = date_part('week', now())-1
	and store_num not in (1,2,3) and fso.macro_level_scm in ('WINTER SPORTS','OUTDOOR','WATER SPORT','FITNESS','RUN& KID& WALKING')
group by 1,2,3,4,5,11,12),
year_2024_r as(
			select rank() over (partition by macro_level_scm, sector order by  to_sum desc) as rn,
			year_2024.*
			from year_2024)
select *
from year_2025_r
where rn<=5
union all 
select *
from year_2024_r
where rn<=5;
