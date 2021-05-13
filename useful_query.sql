1. 등락 빈번한 종목 찾기

select fact.ticker_name, max(min100), max(max100), max(min9), max(max9), sum(case when price > min9 then 1 else 0 end), sum(case when price < max9 then 1 else 0 end) 
from ticker_price_fact fact
left outer join 
(select ticker_name, abs((min(price) - avg(price) )/min(price)*100.0) as minusdelta, 
		min(price) min100, (min(price)*1.02) min9, (max(price)-avg(price))/max(price)*100.0 as plusdelta ,
		max(price) max100, (max(price)*0.98) max9 from ticker_price_fact 
        where sequence_number >= (SELECT last_value-100 FROM sequence_main) group by ticker_name) base
on fact.sequence_number >= (SELECT last_value-100 FROM sequence_main) and fact.ticker_name = base.ticker_name
group by fact.ticker_name
order by sum(case when price > min9 then 1 else 0 end) desc, sum(case when price < max9 then 1 else 0 end) desc;


