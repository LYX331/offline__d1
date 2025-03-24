-- dwd
drop table if exists dwd_order_info;
create external table dwd_order_info (
                                         `id` string COMMENT '',
                                         `total_amount` decimal(10,2) COMMENT '',
                                         `order_status` string COMMENT ' 1 2  3  4  5',
                                         `user_id` string COMMENT 'id' ,
                                         `payment_way` string COMMENT '',
                                         `out_trade_no` string COMMENT '',
                                         `create_time` string COMMENT '',
                                         `operate_time` string COMMENT ''
) COMMENT ''
    PARTITIONED BY ( `dt` string)
    stored as  parquet
    location '/user/hive/warehouse/dev_realtime_v1_yuxin_li.db/dwd/dwd_order_info/'
    TBLPROPERTIES (
        'hive.exec.compress.output' = 'true',
        'mapreduce.output.fileoutputformat.compress' = 'true',
        'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
        );



drop table if exists dwd_order_detail;
create external table dwd_order_detail(
                                          `id` string COMMENT '',
                                          `order_id` decimal(10,2) COMMENT '',
                                          `user_id` string COMMENT 'id' ,
                                          `sku_id` string COMMENT 'id',
                                          `sku_name` string COMMENT '',
                                          `order_price` string COMMENT '',
                                          `sku_num` string COMMENT '',
                                          `create_time` string COMMENT ''
) COMMENT ''
    PARTITIONED BY ( `dt` string)
    stored as  parquet
    location '/user/hive/warehouse/dev_realtime_v1_yuxin_li.db/dwd/dwd_order_detail/'
    TBLPROPERTIES (
        'hive.exec.compress.output' = 'true',
        'mapreduce.output.fileoutputformat.compress' = 'true',
        'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
        );





drop table if exists dwd_user_info;
create external table dwd_user_info(
                                       `id` string COMMENT 'id',
                                       `name`  string COMMENT '',
                                       `birthday` string COMMENT '' ,
                                       `gender` string COMMENT '',
                                       `email` string COMMENT '',
                                       `user_level` string COMMENT '',
                                       `create_time` string COMMENT ''
) COMMENT ''
    PARTITIONED BY ( `dt` string)
    stored as  parquet
    location '/user/hive/warehouse/dev_realtime_v1_yuxin_li.db/dwd/dwd_user_info/'
    TBLPROPERTIES (
        'hive.exec.compress.output' = 'true',
        'mapreduce.output.fileoutputformat.compress' = 'true',
        'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
        );





drop table if exists `dwd_payment_info`;
create external  table  `dwd_payment_info`(
                                              `id`  bigint COMMENT '',
                                              `out_trade_no`  string COMMENT '',
                                              `order_id`  string COMMENT '',
                                              `user_id`  string COMMENT '',
                                              `alipay_trade_no` string COMMENT '',
                                              `total_amount`  decimal(16,2) COMMENT '',
                                              `subject`  string COMMENT '',
                                              `payment_type` string COMMENT '',
                                              `payment_time`  string COMMENT ''
)  COMMENT ''
    PARTITIONED BY ( `dt` string)
    stored as  parquet
    location '/user/hive/warehouse/dev_realtime_v1_yuxin_li.db/dwd/dwd_payment_info/'
    TBLPROPERTIES (
        'hive.exec.compress.output' = 'true',
        'mapreduce.output.fileoutputformat.compress' = 'true',
        'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
        );


drop table if exists dwd_sku_info;
create external table dwd_sku_info(
                                      `id` string COMMENT 'skuId',
                                      `spu_id` string COMMENT 'spuid',
                                      `price` decimal(10,2) COMMENT '' ,
                                      `sku_name` string COMMENT '',
                                      `sku_desc` string COMMENT '',
                                      `weight` string COMMENT '',
                                      `tm_id` string COMMENT 'id',
                                      `category3_id` string COMMENT '1id',
                                      `category2_id` string COMMENT '2id',
                                      `category1_id` string COMMENT '3id',
                                      `category3_name` string COMMENT '3',
                                      `category2_name` string COMMENT '2',
                                      `category1_name` string COMMENT '1',
                                      `create_time` string COMMENT ''
) COMMENT ''
    PARTITIONED BY ( `dt` string)
    stored as  parquet
    location '/user/hive/warehouse/dev_realtime_v1_yuxin_li.db/dwd/dwd_sku_info/'
    TBLPROPERTIES (
        'hive.exec.compress.output' = 'true',
        'mapreduce.output.fileoutputformat.compress' = 'true',
        'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
        );



;


set hive.exec.dynamic.partition.mode=nonstrict;

insert  overwrite table   dwd_order_info partition(dt)
select  * from ods_order_info
where dt='2025-03-23'  and id is not null;

insert  overwrite table  dwd_order_detail partition(dt)
select  * from ods_order_detail
where dt='2025-03-23'   and id is not null;

insert  overwrite table  dwd_user_info partition(dt)
select  * from ods_user_info
where dt='2025-03-23'   and id is not null;

insert  overwrite table   dwd_payment_info partition(dt)
select  * from ods_payment_info
where dt='2025-03-23'  and id is not null;

insert  overwrite table  dwd_sku_info partition(dt)
select
    sku.id,
    sku.spu_id,
    sku.price,
    sku.sku_name,
    sku.sku_desc,
    sku.weight,
    sku.tm_id,
    sku.category3_id,
    c2.id category2_id ,
    c1.id category1_id,
    c3.name category3_name,
    c2.name category2_name,
    c1.name category1_name,
    sku.create_time,
    sku.dt
from
    ods_sku_info sku
        join ods_base_category3 c3 on sku.category3_id=c3.id
        join ods_base_category2 c2 on c3.category2_id=c2.id
        join ods_base_category1 c1 on c2.category1_id=c1.id
where sku.dt='2025-03-23'  and c2.dt='2025-03-23'
  and  c3.dt='2025-03-23' and  c1.dt='2025-03-23'
  and sku.id is not null;