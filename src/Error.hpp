
enum E_ERR_DBAGENT
{
    ERR_INCOMPLET_DATAPROXY_DATA        = 11001,    ///< DataProxy请求数据包不完整
    ERR_INVALID_REDIS_ROUTE             = 11002,    ///< 无效的redis路由信息
    ERR_REDIS_NODE_NOT_FOUND            = 11003,    ///< 未找到合适的redis节点
    ERR_REGISTERCALLBACK_REDIS          = 11004,    ///< 注册RedisStep错误
    ERR_REDIS_CMD                       = 11005,    ///< redis命令执行出错
    ERR_UNEXPECTED_REDIS_REPLY          = 11006,    ///< 不符合预期的redis结果
    ERR_RESULTSET_EXCEED                = 11007,    ///< 数据包超过protobuf最大限制
    ERR_LACK_CLUSTER_INFO               = 11008,    ///< 缺少集群信息
    ERR_TIMEOUT                         = 11009,    ///< 超时
    ERR_REDIS_AND_DB_CMD_NOT_MATCH      = 11010,    ///< redis读写操作与DB读写操作不匹配
    ERR_REDIS_NIL_AND_DB_FAILED         = 11011,    ///< redis结果集为空，但发送DB操作失败
    ERR_NO_RIGHT                        = 11012,    ///< 数据操作权限不足
    ERR_QUERY                           = 11013,    ///< 查询出错，如拼写SQL错误
    ERR_REDIS_STRUCTURE_WITH_DATASET    = 11014,    ///< redis数据结构由DB的各字段值序列化（或串联）而成，请求与存储不符
    ERR_REDIS_STRUCTURE_WITHOUT_DATASET = 11015,    ///< redis数据结构并非由DB的各字段值序列化（或串联）而成，请求与存储不符
    ERR_DB_FIELD_NUM                    = 11016,    ///< redis数据结构由DB的各字段值序列化（或串联）而成，请求的字段数量错误
    ERR_DB_FIELD_ORDER_OR_FIELD_NAME    = 11017,    ///< redis数据结构由DB的各字段值序列化（或串联）而成，请求字段顺序或字段名错误
    ERR_KEY_FIELD                       = 11018,    ///< redis数据结构由DB的各字段值序列化（或串联）而成，指定的key_field错误或未指定，或未>
    ERR_KEY_FIELD_VALUE                 = 11019,    ///< redis数据结构指定的key_field所对应的值缺失或值为空
    ERR_JOIN_FIELDS                     = 11020,    ///< redis数据结构由DB字段串联而成，串联的字段错误
    ERR_LACK_JOIN_FIELDS                = 11021,    ///< redis数据结构由DB字段串联而成，缺失串联字段
    ERR_REDIS_STRUCTURE_NOT_DEFINE      = 11022,    ///< redis数据结构未在DataProxy的配置中定义
    ERR_INVALID_CMD_FOR_HASH_DATASET    = 11023,    ///< redis hash数据结构由DB的各字段值序列化（或串联）而成，而请求中的hash命令不当
    ERR_DB_TABLE_NOT_DEFINE             = 11024,    ///< 表未在DataProxy的配置中定义
    ERR_DB_OPERATE_MISSING              = 11025,    ///< redis数据结构存在对应的DB表，但数据请求缺失对数据库表操作
    ERR_REQ_MISS_PARAM                  = 11026,    ///< 请求参数缺失
    ERR_LACK_TABLE_FIELD                = 11027,    ///< 数据库表字段缺失
    ERR_TABLE_FIELD_NAME_EMPTY          = 11028,    ///< 数据库表字段名为空
    ERR_UNDEFINE_REDIS_OPERATE          = 11029,    ///< 未定义或不支持的redis数据操作（只支持string和hash的dataset update操作）
    ERR_REDIS_READ_WRITE_CMD_NOT_MATCH  = 11030,    ///< redis读命令与写命令不对称
    ERR_PARASE_PROTOBUF                 = 11098,
    ERR_NEW                             = 11099,
};

