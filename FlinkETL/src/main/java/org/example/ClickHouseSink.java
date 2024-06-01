package org.example;

import ru.yandex.clickhouse.ClickHouseDataSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author SummCoder
 * @desc 使用 Flink 的 ClickHouse sink 将数据写入到相应的 ClickHouse 表中。
 * @date 2024/5/30 21:41
 */

public class ClickHouseSink extends RichSinkFunction<Event> {
    private ClickHouseConnection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (connection != null) {
            connection.close();
        }
    }

    @Override
    public void invoke(Event event, Context context) throws Exception {

        String url = "jdbc:clickhouse://localhost:8123/dm";
        ClickHouseProperties properties = new ClickHouseProperties();
        properties.setUser("default");
        properties.setPassword("123456");

        ClickHouseDataSource dataSource = new ClickHouseDataSource(url, properties.asProperties());

        String tableName = getTableName(event.getEventType());
        String[] columns = getColumns(event.getEventType());
        String sql = buildInsertSQL(tableName, columns);
        try {
            connection = dataSource.getConnection();
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            setPreparedStatementValues(preparedStatement, columns, event.getEventBody());
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private String buildInsertSQL(String tableName, String[] columns) {
        StringBuilder sql = new StringBuilder("INSERT INTO ");
        sql.append(tableName).append(" (");
        for (int i = 0; i < columns.length; i++) {
            sql.append(columns[i]);
            if (i < columns.length - 1) {
                sql.append(", ");
            }
        }
        sql.append(") VALUES (");
        for (int i = 0; i < columns.length; i++) {
            sql.append("?");
            if (i < columns.length - 1) {
                sql.append(", ");
            }
        }
        sql.append(")");
        return sql.toString();
    }

    private void setPreparedStatementValues(PreparedStatement preparedStatement, String[] columns, JsonNode eventBody) throws SQLException {
        for (int i = 0; i < columns.length; i++) {
            JsonNode valueNode = eventBody.get(columns[i]);
            if (valueNode != null) {
                // 根据实际数据类型设置PreparedStatement的值
                if (valueNode.isTextual()) {
                    preparedStatement.setString(i + 1, valueNode.asText());
                } else if (valueNode.isInt()) {
                    preparedStatement.setInt(i + 1, valueNode.asInt());
                } else if (valueNode.isDouble()) {
                    preparedStatement.setDouble(i + 1, valueNode.asDouble());
                } else if (valueNode.isBoolean()) {
                    preparedStatement.setBoolean(i + 1, valueNode.asBoolean());
                } else {
                    preparedStatement.setObject(i + 1, valueNode.asText());
                }
            } else {
                // 如果eventBody中没有对应的字段，设置为null
                preparedStatement.setObject(i + 1, null);
            }
        }
    }

    private String getTableName(String eventType) {
        // 根据eventType映射到ClickHouse表名
        switch (eventType) {
            case "contract":
                return "dm_v_tr_contract_mx";
            case "djk":
                return "dm_v_tr_djk_mx";
            case "dsf":
                return "dm_v_tr_dsf_mx";
            case "duebill":
                return "dm_v_tr_duebill_mx";
            case "etc":
                return "dm_v_tr_etc_mx";
            case "grwy":
                return "dm_v_tr_grwy_mx";
            case "gzdf":
                return "dm_v_tr_gzdf_mx";
            case "huanb":
                return "dm_v_tr_huanb_mx";
            case "huanx":
                return "dm_v_tr_huanx_mx";
            case "sa":
                return "dm_v_tr_sa_mx";
            case "sbyb":
                return "dm_v_tr_sbyb_mx";
            case "sdrq":
                return "dm_v_tr_sdrq_mx";
            case "shop":
                return "dm_v_tr_shop_mx";
            case "sjyh":
                return "dm_v_tr_sjyh_mx";
            default:
                throw new IllegalArgumentException("Unknown event type: " + eventType);
        }
    }

    private String[] getColumns(String eventType) {
        // 根据eventType返回相应的字段列表
        switch (eventType) {
            case "contract":
                return new String[]{
                        "uid",           // 证件号码
                        "contract_no",   // 贷款合同号
                        "apply_no",      // 相关申请流水号
                        "artificial_no", // 人工编号
                        "occur_date",    // 发生日期
                        "loan_cust_no",   // 信贷客户号
                        "cust_name",      // 客户名称
                        "buss_type",      // 业务品种
                        "occur_type",     // 发生类型
                        "is_credit_cyc",  // 额度是否循环
                        "curr_type",      // 币种
                        "buss_amt",       // 金额
                        "loan_pert",      // 贷款成数
                        "term_year",      // 期限年
                        "term_mth",       // 期限月
                        "term_day",       // 期限日
                        "base_rate_type", // 基准利率类型
                        "base_rate",      // 基准利率
                        "float_type",     // 浮动类型
                        "rate_float",     // 利率浮动
                        "rate",           // 利率
                        "pay_times",      // 还款期次
                        "pay_type",       // 还款方式
                        "direction",      // 投向
                        "loan_use",       // 用途
                        "pay_source",     // 还款来源
                        "putout_date",    // 发放日期
                        "matu_date",      // 到期日期
                        "vouch_type",     // 主要担保方式
                        "apply_type",     // 申请方式
                        "extend_times",   // 展期次数
                        "actu_out_amt",   // 已实际出帐金额
                        "bal",            // 余额
                        "norm_bal",       // 正常余额
                        "dlay_bal",       // 逾期余额
                        "dull_bal",       // 呆滞余额
                        "owed_int_in",    // 表内欠息金额
                        "owed_int_out",   // 表外欠息余额
                        "fine_pr_int",    // 本金罚息
                        "fine_intr_int",  // 利息罚息
                        "dlay_days",      // 逾期天数
                        "five_class",     // 五级分类
                        "class_date",     // 最新风险分类时间
                        "mge_org",        // 管户机构号
                        "mgr_no",         // 管户人工号
                        "operate_org",    // 经办机构
                        "operator",       // 经办人
                        "operate_date",   // 经办日期
                        "reg_org",        // 登记机构
                        "register",       // 登记人
                        "reg_date",       // 登记日期
                        "inte_settle_type",// 结息方式
                        "is_bad",         // 不良记录标志
                        "frz_amt",        // 冻结金额
                        "con_crl_type",   // 合同控制方式
                        "shift_type",     // 移交类型
                        "due_intr_days",  // 欠息天数
                        "reson_type",     // 原因类型
                        "shift_bal",      // 移交余额
                        "is_vc_vouch",    // 是否担保公司担保
                        "loan_use_add",   // 贷款用途补充
                        "finsh_type",     // 终结类型
                        "finsh_date",     // 终结日期
                        "sts_flag",       // 转建行标志
                        "src_dt",         // 源系统日期
                        "etl_dt"          // 平台日期
                };
            case "djk":
                return new String[]{
                        "uid",          // 证件号码
                        "card_no",      // 卡号
                        "tran_type",    // 交易类型
                        "tran_type_desc",// 交易类型描述
                        "tran_amt",     // 交易金额
                        "tran_amt_sign", // 交易金额符号
                        "mer_type",     // 商户类型
                        "mer_code",     // 商户代码
                        "rev_ind",      // 撤销、冲正标志
                        "tran_desc",    // 交易描述
                        "tran_date",    // 交易日期
                        "val_date",     // 入账日期
                        "pur_date",     // 交易发生日期
                        "tran_time",    // 交易时间
                        "acct_no",      // 账号
                        "etl_dt"        // 数据日期
                };
            case "dsf":
                return new String[]{
                        "tran_date",      // 交易日期
                        "tran_log_no",    // 交易流水号
                        "tran_code",      // 交易代码
                        "channel_flg",    // 渠道
                        "tran_org",       // 交易机构号
                        "tran_teller_no", // 操作柜员号
                        "dc_flag",        // 借贷方标识
                        "tran_amt",       // 交易金额
                        "send_bank",      // 发起行行号
                        "payer_open_bank",// 付款人开户行行号
                        "payer_acct_no",  // 付款人账号
                        "payer_name",     // 付款人名称
                        "payee_open_bank",// 收款人开户行行号
                        "payee_acct_no",  // 收款人账号
                        "payee_name",     // 收款人名称
                        "tran_sts",       // 交易状态
                        "busi_type",      // 业务类型
                        "busi_sub_type",  // 业务种类
                        "etl_dt",         // 数据日期
                        "uid"             // 证件号码
                };
            case "duebill":
                return new String[]{
                        "uid",            // 证件号码
                        "acct_no",        // 账号
                        "receipt_no",     // 借据流水号
                        "contract_no",    // 贷款合同号
                        "subject_no",     // 科目号
                        "cust_no",        // 核心客户号
                        "loan_cust_no",   // 信贷客户号
                        "cust_name",      // 客户名称
                        "buss_type",      // 业务品种
                        "curr_type",      // 币种
                        "buss_amt",       // 金额
                        "putout_date",    // 发放日期
                        "matu_date",      // 约定到期日
                        "actu_matu_date", // 执行到期日
                        "buss_rate",      // 利率
                        "actu_buss_rate", // 执行利率
                        "intr_type",      // 计息方式
                        "intr_cyc",       // 计息周期
                        "pay_times",      // 还款期次
                        "pay_cyc",        // 还款周期
                        "extend_times",   // 展期次数
                        "bal",            // 余额
                        "norm_bal",       // 正常余额
                        "dlay_amt",       // 逾期金额
                        "dull_amt",       // 呆滞金额
                        "bad_debt_amt",   // 呆帐金额
                        "owed_int_in",    // 表内欠息金额
                        "owed_int_out",   // 表外欠息金额
                        "fine_pr_int",    // 本金罚息
                        "fine_intr_int",  // 利息罚息
                        "dlay_days",      // 逾期天数
                        "pay_acct",       // 存款帐号
                        "putout_acct",    // 放款账号
                        "pay_back_acct",  // 还款帐号
                        "due_intr_days",  // 欠息天数
                        "operate_org",    // 经办机构
                        "operator",       // 经办人
                        "reg_org",        // 登记机构
                        "register",       // 登记人
                        "occur_date",     // 发生日期
                        "loan_use",       // 贷款用途
                        "pay_type",       // 还款方式
                        "pay_freq",       // 还款频率
                        "vouch_type",     // 主要担保方式
                        "mgr_no",         // 管户人工号
                        "mge_org",        // 管户机构号
                        "loan_channel",   // 贷款渠道
                        "ten_class",      // 新十级分类编码
                        "src_dt",         // 源系统日期
                        "etl_dt"          // 平台日期
                };
            case "etc":
                return new String[]{
                        "uid",           // 证件号码
                        "etc_acct",       // ETC账号
                        "card_no",        // 卡号
                        "car_no",         // 车牌号
                        "cust_name",      // 客户名称
                        "tran_date",      // 交易日期
                        "tran_time",      // 交易时间
                        "tran_amt_fen",   // 交易金额
                        "real_amt",       // 实收金额
                        "conces_amt",     // 优惠金额
                        "tran_place",     // 通行路程
                        "mob_phone",      // 手机号码
                        "etl_dt"          // 数据日期
                };
            case "grwy":
            case "sjyh":
                return new String[]{
                        "uid",            // 证件号码
                        "mch_channel",    // 模块渠道代号
                        "login_type",     // 登录类型
                        "ebank_cust_no",  // 电子银行客户号
                        "tran_date",      // 交易日期
                        "tran_time",      // 交易时间
                        "tran_code",      // 交易代码
                        "tran_sts",       // 交易状态
                        "return_code",    // 返回码
                        "return_msg",     // 返回信息
                        "sys_type",       // 业务系统类型
                        "payer_acct_no",  // 付款人账号
                        "payer_acct_name",// 转出户名
                        "payee_acct_no",  // 收款人账号
                        "payee_acct_name",// 收款人户名
                        "tran_amt",       // 交易金额
                        "etl_dt"          // 数据日期
                };
            case "gzdf":
                return new String[]{
                        "belong_org",     // 归属机构号
                        "ent_acct",       // 企业账号
                        "ent_name",       // 企业名称
                        "eng_cert_no",    // 企业证件号码
                        "acct_no",        // 账号
                        "cust_name",      // 客户名称
                        "uid",            // 证件号码
                        "tran_date",      // 交易日期
                        "tran_amt",       // 交易金额
                        "tran_log_no",    // 交易流水号
                        "is_secu_card",   // 是否社保卡
                        "trna_channel",   // 代发渠道
                        "batch_no",       // 批次号
                        "etl_dt"          // 数据日期
                };
            case "huanb":
                return new String[]{
                        "tran_flag",       // 还本标志
                        "uid",             // 证件号码
                        "cust_name",       // 客户名称
                        "acct_no",         // 账号
                        "tran_date",       // 交易日期
                        "tran_time",       // 交易时间
                        "tran_amt",        // 交易金额
                        "bal",             // 余额
                        "tran_code",       // 交易代码
                        "dr_cr_code",      // 借贷别
                        "pay_term",        // 还款期数
                        "tran_teller_no",  // 操作柜员号
                        "pprd_rfn_amt",    // 每期还款金额
                        "pprd_amotz_intr", // 每期摊还额计算利息
                        "tran_log_no",     // 交易流水号
                        "tran_type",       // 交易类型
                        "dscrp_code",      // 摘要
                        "remark",          // 备注
                        "etl_dt"           // 数据日期
                };
            case "huanx":
                return new String[]{
                        "tran_flag",       // 还息类型
                        "uid",             // 证件号码
                        "cust_name",       // 客户名称
                        "acct_no",         // 账号
                        "tran_date",       // 交易日期
                        "tran_time",       // 交易时间
                        "tran_amt",        // 利息
                        "cac_intc_pr",     // 计息本金
                        "tran_code",       // 交易代码
                        "dr_cr_code",      // 借贷别
                        "pay_term",        // 还款期数
                        "tran_teller_no",  // 操作柜员号
                        "intc_strt_date",  // 计息起始日期
                        "intc_end_date",   // 计息截止日期
                        "intr",            // 利率
                        "tran_log_no",     // 交易流水号
                        "tran_type",       // 交易类型
                        "dscrp_code",      // 摘要
                        "etl_dt"           // 数据日期
                };
            case "sa":
                return new String[]{
                        "uid",             // 证件号码
                        "card_no",         // 卡号
                        "cust_name",       // 客户名称
                        "acct_no",         // 账号
                        "det_n",           // 活存帐户明细号
                        "curr_type",       // 币种
                        "tran_teller_no",  // 操作柜员号
                        "cr_amt",          // 贷方发生额
                        "bal",             // 余额
                        "tran_amt",        // 交易金额
                        "tran_card_no",    // 交易卡号
                        "tran_type",       // 交易类型
                        "tran_log_no",     // 交易流水号
                        "dr_amt",          // 借方发生额
                        "open_org",        // 开户机构号
                        "dscrp_code",      // 摘要
                        "remark",          // 备注
                        "tran_time",       // 交易时间
                        "tran_date",       // 交易日期
                        "sys_date",        // 系统日期
                        "tran_code",       // 交易代码
                        "remark_1",        // 备注_1
                        "oppo_cust_name",  // 对方户名
                        "agt_cert_type",   // 代理人证件种类
                        "agt_cert_no",     // 代理人证件号
                        "agt_cust_name",   // 代理人名称
                        "channel_flag",    // 渠道标志
                        "oppo_acct_no",    // 对方账号
                        "oppo_bank_no",    // 对方行号
                        "src_dt",          // 源系统日期
                        "etl_dt"           // 数据日期
                };
            case "sbyb":
                return new String[]{
                        "uid",             // 证件号码
                        "cust_name",       // 客户名称
                        "tran_date",       // 交易日期
                        "tran_sts",        // 交易状态
                        "tran_org",        // 交易机构号
                        "tran_teller_no",  // 操作柜员号
                        "tran_amt_fen",    // 交易金额
                        "tran_type",       // 交易类型
                        "return_msg",      // 返回信息
                        "etl_dt"           // 数据日期
                };
            case "sdrq":
                return new String[]{
                        "hosehld_no",      // 户号
                        "acct_no",         // 账号
                        "cust_name",       // 客户名称
                        "tran_type",       // 交易类型
                        "tran_date",       // 交易日期
                        "tran_amt_fen",    // 交易金额
                        "channel_flg",     // 渠道
                        "tran_org",        // 交易机构号
                        "tran_teller_no",  // 操作柜员号
                        "tran_log_no",     // 交易流水号
                        "batch_no",        // 批次号
                        "tran_sts",        // 交易状态
                        "return_msg",      // 返回信息
                        "etl_dt",          // 数据日期
                        "uid"              // 证件号码
                };
            case "shop":
                return new String[]{
                        "tran_channel",    // 交易渠道
                        "order_code",      // 订单号
                        "shop_code",       // 商户码
                        "shop_name",       // 商户名称
                        "hlw_tran_type",   // 交易类型
                        "tran_date",       // 交易日期
                        "tran_time",       // 交易时间
                        "tran_amt",        // 交易金额
                        "current_status",  // 交易状态
                        "score_num",       // 优惠积分
                        "pay_channel",     // 支付渠道
                        "uid",             // 负责人证件号码
                        "legal_name",      // 负责人名称
                        "etl_dt"           // 数据跑批日期
                };
            default:
                throw new IllegalArgumentException("Unknown event type: " + eventType);
        }
    }

}
