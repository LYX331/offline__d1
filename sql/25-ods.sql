CREATE TABLE `ods_order_info` (
                                  `id` int   COMMENT 'id',
                                  `order_no` string  COMMENT '订单号',
                                  `status` string     COMMENT '订单状态',
                                  `collect_type` string   COMMENT '取件类型，1为网点自寄，2为上门取件',
                                  `user_id` int  COMMENT '客户id',
                                  `receiver_complex_id` int  COMMENT '收件人小区id',
                                  `receiver_province_id` string  COMMENT '收件人省份id',
                                  `receiver_city_id` string   COMMENT '收件人城市id',
                                  `receiver_district_id` string  COMMENT '收件人区县id',
                                  `receiver_address` string  COMMENT '收件人详细地址',
                                  `receiver_name` string  COMMENT '收件人姓名',
                                  `receiver_phone` string  COMMENT '收件人电话',
                                  `receive_location` string  COMMENT '起始点经纬度',
                                  `sender_complex_id` int  COMMENT '发件人小区id',
                                  `sender_province_id` string  COMMENT '发件人省份id',
                                  `sender_city_id` string  COMMENT '发件人城市id',
                                  `sender_district_id` string  COMMENT '发件人区县id',
                                  `sender_address` string  COMMENT '发件人详细地址',
                                  `sender_name` string  COMMENT '发件人姓名',
                                  `sender_phone` string  COMMENT '发件人电话',
                                  `send_location` string  COMMENT '发件人坐标',
                                  `payment_type` string  COMMENT '支付方式',
                                  `cargo_num` int  COMMENT '货物个数',
                                  `amount` decimal(32,2)  COMMENT '金额',
                                  `estimate_arrive_time` date  COMMENT '预计到达时间',
                                  `distance` decimal(10,2)  COMMENT '距离，单位：公里',
                                  `create_time` date  COMMENT '创建时间',
                                  `update_time` date   COMMENT '更新时间',
                                  `is_deleted` string COMMENT '删除标记（0:不可用 1:可用）'
)

    COMMENT '订单表'
    PARTITIONED BY (`dt` string)
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\t'
    LOCATION '/user/hive/warehouse/dev_realtime_v2_yuxin_li.db/ods/ods_order_info/'
    TBLPROPERTIES (
        'hive.exec.compress.output' = 'true',
        'mapreduce.output.fileoutputformat.compress' = 'true',
        'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
        );



load data inpath '/2207A/liyuxin/tms01/order_info/2025-03-24'
    OVERWRITE into table
    ods_order_info partition (dt='2025-03-24');



CREATE TABLE `ods_order_cargo` (
                                   `id` int  ,
                                   `order_id` string  COMMENT '订单id',
                                   `cargo_type` string   COMMENT '货物类型id',
                                   `volume_length` int   COMMENT '长cm',
                                   `volume_width` int   COMMENT '宽cm',
                                   `volume_height` int   COMMENT '高cm',
                                   `weight` decimal(16,2)   COMMENT '重量 kg',
                                   `remark` string COMMENT '备注',
                                   `create_time` string  COMMENT '创建时间',
                                   `update_time` string    COMMENT '更新时间',
                                   `is_deleted` string  COMMENT '删除标记（0:不可用 1:可用）'
) PARTITIONED BY (`dt` string)
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\t'
    LOCATION '/user/hive/warehouse/dev_realtime_v2_yuxin_li.db/ods/ods_order_cargo/'
    TBLPROPERTIES (
        'hive.exec.compress.output' = 'true',
        'mapreduce.output.fileoutputformat.compress' = 'true',
        'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
        );

load data inpath '/2207A/liyuxin/tms01/order_cargo/2025-03-24'
    OVERWRITE into table
    ods_order_cargo partition (dt='2025-03-24');



CREATE TABLE `ods_base_region_info` (
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
    LOCATION '/user/hive/warehouse/dev_realtime_v2_yuxin_li.db/ods/ods_base_region_info/'
    TBLPROPERTIES (
        'hive.exec.compress.output' = 'true',
        'mapreduce.output.fileoutputformat.compress' = 'true',
        'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
        );

load data inpath '/2207A/liyuxin/tms01/base_region_info/2025-03-24'
    OVERWRITE into table
    ods_base_region_info partition (dt='2025-03-24');



CREATE TABLE `ods_base_organ` (
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
    LOCATION '/user/hive/warehouse/dev_realtime_v2_yuxin_li.db/ods/ods_base_organ/'
    TBLPROPERTIES (
        'hive.exec.compress.output' = 'true',
        'mapreduce.output.fileoutputformat.compress' = 'true',
        'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
        );

load data inpath '/2207A/liyuxin/tms01/base_organ/2025-03-24'
    OVERWRITE into table
    ods_base_organ partition (dt='2025-03-24');


CREATE TABLE `ods_base_dic` (
                                `id` int    COMMENT 'id',
                                `parent_id` int    COMMENT '上级id',
                                `name` string    COMMENT '名称',
                                `dict_code` string   COMMENT '编码',
                                `create_time` string    COMMENT '创建时间',
                                `update_time` string    COMMENT '更新时间',
                                `is_deleted` int  COMMENT '删除标记（0:不可用 1:可用）'
) PARTITIONED BY (`dt` string)
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\t'
    LOCATION '/user/hive/warehouse/dev_realtime_v2_yuxin_li.db/ods/ods_base_dic/'
    TBLPROPERTIES (
        'hive.exec.compress.output' = 'true',
        'mapreduce.output.fileoutputformat.compress' = 'true',
        'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
        );

load data inpath '/2207A/liyuxin/tms01/base_dic/2025-03-24'
    OVERWRITE into table
    ods_base_dic partition (dt='2025-03-24');



CREATE TABLE `ods_base_complex` (
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
    LOCATION '/user/hive/warehouse/dev_realtime_v2_yuxin_li.db/ods/ods_base_complex/'
    TBLPROPERTIES (
        'hive.exec.compress.output' = 'true',
        'mapreduce.output.fileoutputformat.compress' = 'true',
        'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
        );


load data inpath '/2207A/liyuxin/tms01/base_complex/2025-03-24'
    OVERWRITE into table
    ods_base_complex partition (dt='2025-03-24');