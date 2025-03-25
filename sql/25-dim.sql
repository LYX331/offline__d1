CREATE TABLE `dim_base_complex` (
                                    `id` int  ,
                                    `complex_name` string  ,
                                    `province_id` int  ,
                                    `city_id` int  ,
                                    `district_id` int  ,
                                    `district_name` string   ,
                                    `create_time` date  ,
                                    `update_time` date  ,
                                    `is_deleted` string
)  PARTITIONED BY (`dt` string)
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\t'
    LOCATION '/user/hive/warehouse/dev_realtime_v2_yuxin_li.db/dim/dim_base_complex/'
    TBLPROPERTIES (
        'hive.exec.compress.output' = 'true',
        'mapreduce.output.fileoutputformat.compress' = 'true',
        'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
        );




CREATE TABLE `dim_base_region_info` (
                                        `id` int    COMMENT 'id',
                                        `parent_id` int    COMMENT '上级id',
                                        `name` string    COMMENT '名称',
                                        `dict_code` string   COMMENT '编码',
                                        `short_name` string   COMMENT '简称',
                                        `create_time` string    COMMENT '创建时间',
                                        `update_time` string  COMMENT '更新时间',
                                        `is_deleted` int COMMENT '删除标记（0:不可用 1:可用）'
) PARTITIONED BY (`dt` string)
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\t'
    LOCATION '/user/hive/warehouse/dev_realtime_v2_yuxin_li.db/ods/dim_base_region_info/'
    TBLPROPERTIES (
        'hive.exec.compress.output' = 'true',
        'mapreduce.output.fileoutputformat.compress' = 'true',
        'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
        );

CREATE TABLE `dim_base_organ` (
                                  `id` int  ,
                                  `org_name` string  COMMENT '机构名称',
                                  `org_level` int   COMMENT '行政级别',
                                  `region_id` int   COMMENT '区域id，1级机构为city ,2级机构为district',
                                  `org_parent_id` int   COMMENT '上级机构id',
                                  `points` string COMMENT '多边形经纬度坐标集合',
                                  `create_time` string    COMMENT '创建时间',
                                  `update_time` string    COMMENT '更新时间',
                                  `is_deleted` string    COMMENT '删除标记（0:不可用 1:可用）'
) PARTITIONED BY (`dt` string)
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\t'
    LOCATION '/user/hive/warehouse/dev_realtime_v2_yuxin_li.db/ods/dim_base_organ/'
    TBLPROPERTIES (
        'hive.exec.compress.output' = 'true',
        'mapreduce.output.fileoutputformat.compress' = 'true',
        'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
        );


set hive.exec.dynamic.partition.mode=nonstrict;

insert  overwrite table   dim_base_organ partition(dt)
select  * from ods_base_organ
where dt='2025-03-24'  and id is not null;

insert  overwrite table  dim_base_region_info partition(dt)
select  * from ods_base_region_info
where dt='2025-03-24'   and id is not null;