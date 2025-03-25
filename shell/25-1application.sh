# 外部配置打开
#logging.config=./logback.xml
#业务日期
mock.date: "2025-03-25"

logging:
  level:
    com.bw.tms: warn

spring:
  datasource:
    url: jdbc:mysql://10.39.48.36:3306/tms01?characterEncoding=utf-8&useSSL=false&serverTimezone=GMT%2B8
    username: root
    password: "Zh1028,./"
    driver-class-name: com.mysql.jdbc.Driver


mybatis-plus.global-config.db-config.field-strategy: not_null
mybatis-plus:
  mapper-locations: classpath:mapper/*.xml


# 清空所有维度数据
mock.reset-all-dim: 1
# 清空所有业务事实数据
mock.clear.busi: 1

# 每6分钟产生订单数
mock.order.count: 1

# 生成新用户  -- 即使维度不清空 用户也可以新增
mock.new.user: 100


#小区数量-- 会影响快递分布的密度 只有维度重建时会重新生成小区
mock.address.complex.num: 100




