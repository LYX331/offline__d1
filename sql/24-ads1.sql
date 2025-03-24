-- ads

-- 第3章GMV成交总额
-- gmv=销售额+取消订单金额+拒收订单金额+退货订单金额
drop table if exists ads_gmv_sum_day;
create table ads_gmv_sum_day(
                                `dt` string COMMENT '统计日期',
                                `gmv_count`  bigint COMMENT '当日gmv订单个数',
                                `gmv_amount`  decimal(16,2) COMMENT '当日gmv订单总金额',
                                `gmv_payment`  decimal(16,2) COMMENT '当日支付金额'
) COMMENT '每日活跃用户数量'
    row format delimited  fields terminated by '\t'
    location '/user/hive/warehouse/dev_realtime_v1_yuxin_li.db/ads/ads_gmv_sum_day/'
;

insert into table ads_gmv_sum_day
select
    '2025-03-23' dt ,
    sum(order_count)  gmv_count ,
    sum(order_amount) gmv_amount ,
    sum(payment_amount) payment_amount
from dws_user_action
where dt ='2025-03-23'
group by dt
;