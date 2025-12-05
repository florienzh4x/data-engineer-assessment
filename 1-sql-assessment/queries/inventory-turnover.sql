with branch_inventory as (
	select 
		bd.branch_name,
		SUM(case when im.transaction_type = 'IN'  then im.quantity ELSE 0 END) AS total_in,
	    SUM(case when im.transaction_type = 'OUT' then im.quantity ELSE 0 END) AS total_out,
	    SUM(case when im.transaction_type = 'EXP' then im.quantity ELSE 0 END) AS expired_items
	from inventory_movements im 
	join branch_details bd 
	 on im.branch_code = bd.branch_code
	where im.transaction_date >= CURRENT_DATE - INTERVAL '3 months'
	group by bd.branch_name
), 
cal_stock as (
	select 
		bi.*,
		(bi.total_in - bi.total_out - bi.expired_items) as current_stock
	from branch_inventory bi
),
turnover as (
	select 
		cs.*,
		(cs.total_out::decimal / NULLIF(total_in, 0)) as turnover_rate
	from cal_stock cs
) select 
	* 
from turnover 
order by turnover_rate desc nulls LAST;