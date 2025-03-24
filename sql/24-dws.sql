-- dws

-- dws
drop table if exists dws_user_action;
create  external table dws_user_action
(
    user_id         string      comment '用户 id',
    order_count     bigint      comment '下单次数 ',
    order_amount    decimal(16,2)  comment '下单金额 ',
    payment_count   bigint      comment '支付次数',
    payment_amount  decimal(16,2) comment '支付金额 ',
    comment_count   bigint      comment '评论次数'
) COMMENT '每日用户行为宽表'
    PARTITIONED BY ( `dt` string)
    stored as  parquet
    location '/user/hive/warehouse/dev_realtime_v1_yuxin_li.db/dws/dws_user_action/'
    TBLPROPERTIES (
        'hive.exec.compress.output' = 'true',
        'mapreduce.output.fileoutputformat.compress' = 'true',
        'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
        );



with
    tmp_order as
        (
            select
                user_id,
                sum(oc.total_amount) order_amount,
                count(*)  order_count
            from dwd_order_info  oc
            where date_format(oc.create_time,'yyyy-MM-dd')='2024-04-01'
            group by user_id
        )  ,
    tmp_payment as
        (
            select
                user_id,
                sum(pi.total_amount) payment_amount,
                count(*) payment_count
            from dwd_payment_info pi
            where date_format(pi.payment_time,'yyyy-MM-dd')='2024-04-01'
            group by user_id
        ),
    tmp_comment as
        (
            select
                user_id,
                count(*) comment_count
            from dwd_comment_log c
            where date_format(c.dt,'yyyy-MM-dd')='2025-02-25'
            group by user_id
        )

insert overwrite table dws_user_action partition(dt='2025-03-23')
select
    user_actions.user_id,
    sum(user_actions.order_count),
    sum(user_actions.order_amount),
    sum(user_actions.payment_count),
    sum(user_actions.payment_amount),
    sum(user_actions.comment_count)
from
    (
        select
            user_id,
            order_count,
            order_amount ,
            0 payment_count ,
            0 payment_amount,
            0 comment_count
        from tmp_order

        union all
        select
            user_id,
            0,
            0,
            payment_count,
            payment_amount,
            0
        from tmp_payment

        union all
        select
            user_id,
            0,
            0,
            0,
            0,
            comment_count
        from tmp_comment
    ) user_actions
group by user_id;


-- dws-- 用户购买商品明细表
drop table if exists dws_sale_detail_daycount;
create external table  dws_sale_detail_daycount
(  user_id string  comment '用户 id',
   sku_id  string comment '商品 Id',
   user_gender  string comment '用户性别',
   user_age string  comment '用户年龄',
   user_level string comment '用户等级',
   order_price decimal(10,2) comment '订单价格',
   sku_name string  comment '商品名称',
   sku_tm_id string  comment '品牌id',
   sku_category3_id string comment '商品三级品类id',
   sku_category2_id string comment '商品二级品类id',
   sku_category1_id string comment '商品一级品类id',
   sku_category3_name string comment '商品三级品类名称',
   sku_category2_name string comment '商品二级品类名称',
   sku_category1_name string comment '商品一级品类名称',
   spu_id  string comment '商品 spu',
   sku_num  int comment '购买个数',
   order_count string comment '当日下单单数',
   order_amount string comment '当日下单金额'
) COMMENT '用户购买商品明细表'
    PARTITIONED BY ( `dt` string)
    stored as  parquet
    location '/user/hive/warehouse/dev_realtime_v1_yuxin_li.db/dws/dws_user_sale_detail_daycount/'
    TBLPROPERTIES (
        'hive.exec.compress.output' = 'true',
        'mapreduce.output.fileoutputformat.compress' = 'true',
        'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
        );


with
    tmp_detail as
        (
            select
                user_id,
                sku_id,
                sum(sku_num) sku_num ,
                count(*) order_count ,
                sum(od.order_price*sku_num)  order_amount
            from ods_order_detail od
            where od.dt='2025-03-23' and user_id is not null
            group by user_id, sku_id
        )
insert overwrite table  dws_sale_detail_daycount partition(dt='2025-03-23')
select
    tmp_detail.user_id,
    tmp_detail.sku_id,
    u.gender,
    months_between('2025-03-23', u.birthday)/12  age,
    u.user_level,
    price,
    sku_name,
    tm_id,
    category3_id ,
    category2_id ,
    category1_id ,
    category3_name ,
    category2_name ,
    category1_name ,
    spu_id,
    tmp_detail.sku_num,
    tmp_detail.order_count,
    tmp_detail.order_amount
from tmp_detail
         left join dwd_user_info u on u.id=tmp_detail.user_id  and u.dt='2025-03-23'
         left join dwd_sku_info s on tmp_detail.sku_id =s.id  and s.dt='2025-03-23'
;
