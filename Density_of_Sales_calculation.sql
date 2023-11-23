--Density calculation

--Retail calculation for 33 selected stores

Select 
  'Retail' as scope,
  count(distinct business_unit) as bu_count,
  round((sum(sku_count)*100/(33*30000*365)),1) as density,
From (Select sales.date, -- в день
    sales.business_unit, --в одном магазине
    count(distinct sales.sku_num_sku_r3) as sku_count --факт продажи
From `data-ru-2dlj.finance_wh.raw_sales_per_sku` sales
Where sales.date>='2021-01-01' and sales.date<='2021-12-31'
  and sales.business_unit in (399,885,763,452,2516,551,797,703,838,646,1003,1004,1395,886,1014,1006,1005,2514,2518,1401,894,1038,1361,893,1360,2043,2044,1527,2042,2521,1010,1396,2277) 
  and sales.qty_goods>0
Group by 1,2) as a 


--Retail+ecom+mp calculation for 33 selected stores +(112-decathlon.ru, 246-ozon.ru)

Union all

Select 
  'Retail+ecom' as scope,
  count(distinct business_unit) as bu_count,
  round((sum(sku_count)*100/(35*30000*365)),1) as density,
From (Select sales.date,
    sales.business_unit,
    count(distinct sales.sku_num_sku_r3) as sku_count
From `data-ru-2dlj.finance_wh.raw_sales_per_sku` sales
Where sales.date>='2021-01-01' and sales.date<='2021-12-31'
  and sales.business_unit in (399,885,763,452,2516,551,797,703,838,646,1003,1004,1395,886,1014,1006,1005,2514,2518,1401,894,1038,1361,893,1360,2043,2044,1527,2042,2521,1010,1396,2277,112,246) 
  and sales.qty_goods>0
  and sales.channel in ('offline','marketplace','Ecom')
Group by 1,2) as a 

--Ecom+mp calculation for (112-decathlon.ru, 246-ozon.ru )

Union all

Select 
  'Ecom' as scope,
  count(distinct business_unit) as bu_count,
  round((sum(sku_count)*100/(2*30000*365)),1) as density,
From (Select sales.date,
    sales.business_unit,
    count(distinct sales.sku_num_sku_r3) as sku_count
From `data-ru-2dlj.finance_wh.raw_sales_per_sku` sales
Where sales.date>='2021-01-01' and sales.date<='2021-12-31'
  and sales.business_unit in (112,246) 
  and sales.qty_goods>0
  and sales.channel in ('marketplace','Ecom')
Group by 1,2) as a


