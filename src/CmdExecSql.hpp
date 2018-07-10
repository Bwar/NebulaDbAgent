/*******************************************************************************
 * Project:  NebulaDbAgent
 * @file     CmdExecSql.hpp
 * @brief 
 * @author   Bwar
 * @date:    2016年7月28日
 * @note
 * Modify history:
 ******************************************************************************/
#ifndef CMDEXECSQL_HPP_
#define CMDEXECSQL_HPP_

#include "Error.hpp"
#include "util/json/CJsonObject.hpp"
#include "actor/cmd/Cmd.hpp"

#include "dbi/MysqlDbi.hpp"
#include "pb/mydis.pb.h"

namespace dbagent
{

const int gc_iMaxBeatTimeInterval = 30;
const int gc_iMaxColValueSize = 65535;

struct tagConnection
{
    CMysqlDbi* pDbi;
    time_t ullBeatTime;
    int iQueryPermit;
    int iTimeout;

    tagConnection() : pDbi(NULL), ullBeatTime(0), iQueryPermit(0), iTimeout(0)
    {
    }

    ~tagConnection()
    {
        if (pDbi != NULL)
        {
            delete pDbi;
            pDbi = NULL;
        }
    }
};

class CmdExecSql : public neb::Cmd, public neb::DynamicCreator<CmdExecSql, int32>
{
public:
    CmdExecSql(int32 iCmd);
    virtual ~CmdExecSql();

    virtual bool Init();

    virtual bool AnyMessage(
                    std::shared_ptr<neb::SocketChannel> pChannel, 
                    const MsgHead& oMsgHead,
                    const MsgBody& oMsgBody);

protected:
    bool GetDbConnection(const neb::Mydis& oQuery, CMysqlDbi** ppMasterDbi, CMysqlDbi** ppSlaveDbi);
    bool FetchOrEstablishConnection(neb::Mydis::DbOperate::E_QUERY_TYPE eQueryType,
                    const std::string& strMasterIdentify, const std::string& strSlaveIdentify,
                    const neb::CJsonObject& oInstanceConf, CMysqlDbi** ppMasterDbi, CMysqlDbi** ppSlaveDbi);
    std::string GetFullTableName(const std::string& strTableName, uint32 uiFactor);

    int ConnectDb(const neb::CJsonObject& oInstanceConf, CMysqlDbi* pDbi, bool bIsMaster = true);
    int Query(const neb::Mydis& oQuery, CMysqlDbi* pDbi);
    void CheckConnection(); //检查连接是否已超时
    void Response(int iErrno, const std::string& strErrMsg);
    bool Response(const neb::Result& oRsp);

    bool CreateSql(const neb::Mydis& oQuery, CMysqlDbi* pDbi, std::string& strSql);
    bool CreateSelect(const neb::Mydis& oQuery, std::string& strSql);
    bool CreateInsert(const neb::Mydis& oQuery, CMysqlDbi* pDbi, std::string& strSql);
    bool CreateUpdate(const neb::Mydis& oQuery, CMysqlDbi* pDbi, std::string& strSql);
    bool CreateDelete(const neb::Mydis& oQuery, std::string& strSql);
    bool CreateCondition(const neb::Mydis::DbOperate::Condition& oCondition, CMysqlDbi* pDbi, std::string& strCondition);
    bool CreateConditionGroup(const neb::Mydis& oQuery, CMysqlDbi* pDbi, std::string& strCondition);
    bool CreateGroupBy(const neb::Mydis& oQuery, std::string& strGroupBy);
    bool CreateOrderBy(const neb::Mydis& oQuery, std::string& strOrderBy);
    bool CreateLimit(const neb::Mydis& oQuery, std::string& strLimit);
    bool CheckColName(const std::string& strColName);

private:
    std::shared_ptr<neb::SocketChannel> m_pChannel;
    MsgHead m_oInMsgHead;
    MsgBody m_oInMsgBody;
    int m_iConnectionTimeout;   //空闲连接超时（单位秒）
    char* m_szColValue;         //字段值
    neb::CJsonObject m_oDbConf;
    uint32 m_uiSectionFrom;
    uint32 m_uiSectionTo;
    uint32 m_uiHash;
    uint32 m_uiDivisor;
    std::map<std::string, std::set<uint32> > m_mapFactorSection; //分段因子区间配置，key为因子类型
    std::map<std::string, neb::CJsonObject*> m_mapDbInstanceInfo;  //数据库配置信息key为("%u:%u:%u", uiDataType, uiFactor, uiFactorSection)
    std::map<std::string, tagConnection*> m_mapDbiPool;     //数据库连接池，key为identify（如：192.168.18.22:3306）
};

} // namespace dbagent

#endif 


